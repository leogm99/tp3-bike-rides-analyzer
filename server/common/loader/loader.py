import multiprocessing
import os
import socket
import logging
import queue
import threading
import uuid
import time

from common.loader.loader_middleware import LoaderMiddleware
from common.loader.metrics_waiter import MetricsWaiter
from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
from common_utils.utils import recv_n_bytes
from common.dag_node import DAGNode
from common.loader.static_data_ack_waiter import StaticDataAckWaiter
from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware
from common_utils.protocol.message import Message, TRIPS, WEATHER, STATIONS
from common_utils.protocol.protocol import Protocol
from common.loader.stream_state import StreamState

class Loader(DAGNode):
    def __init__(self,
                 port: int,
                 backlog: int,
                 stations_consumer_replica_count: int,
                 weather_consumer_replica_count: int,
                 trips_consumer_replica_count: int,
                 ack_count: int,
                 middleware_callback,
                 hostname: str,
                 max_clients: int):
        super().__init__()
        try:
            self._socket = None
            self._port = port
            self._backlog = backlog

            self._middleware_callback = middleware_callback

            self._stations_consumer_replica_count = stations_consumer_replica_count
            self._weather_consumer_replica_count = weather_consumer_replica_count
            self._trips_consumer_replica_count = trips_consumer_replica_count

            self._ack_count = ack_count
            self._hostname = hostname
            self._clients_number = max_clients

            self._process_queue = multiprocessing.Queue()

            self._middleware: LoaderMiddleware = self._middleware_callback()
            self._send_flush()

        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se
        except BaseException as e:
            logging.error(f'action: loader-init | status: failed | reason: {e}')
            raise e

    def run(self):
        sem = threading.BoundedSemaphore(value=self._clients_number)
        try:
            _processes = []
            for _ in range(self._clients_number):
                p = threading.Thread(target=self.process_loop, args=(sem,))
                p.start()
                _processes.append(p)

            self.accept_clients(sem)
        except BrokenPipeError:
            logging.info('action: receive_client_message | connection closed by client')
        except BaseException as e:
            if not self.closed:
                raise e from e
        finally:
            if not self.closed:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
                super(Loader, self).close()

    def accept_clients(self, sem):
        self._open_server_socket()
        while True:
            logging.info('action: Accepting clients | status: in progress')
            client_socket, _ = self._socket.accept()
            self._process_queue.put(client_socket)
            sem.acquire()

    def _open_server_socket(self):
        self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
        self._socket.bind(('', self._port))
        self._socket.listen(self._backlog)

    def process_loop(self, sem):
        while True:
            client_socket = self._process_queue.get(block=True, timeout=None)
            
            self._run(client_socket, self._hostname, self._ack_count)
            sem.release()

    def _run(self, client_socket, hostname, ack_count):
        middleware = self._middleware_callback()
        process_id = os.getpid()
        try: 
            logging.info(f'action: run process to receive client data | status: in progress | process_id: {process_id}')

            client_id = self.get_client_id()

            static_ack_waiter = StaticDataAckWaiter(ack_count, middleware=StaticDataAckWaiterMiddleware(
                hostname=hostname, client_id=client_id.payload.data
            ))
            local_queue = queue.Queue()
            metrics_waiter = MetricsWaiter(local_queue=local_queue, middleware=MetricsWaiterMiddleware(
                hostname=hostname, client_id=client_id.payload.data
            ))
            static_ack_waiter.start()
            metrics_waiter.start()

            Protocol.send_message(client_socket.sendall, client_id)

            stream_state = StreamState()

            while stream_state.not_static_data_eof_received():
                self.__receive_client_message_and_publish(client_socket, stream_state, middleware)
            static_ack_waiter.join()
            ack = Message.build_ack_message(client_id.payload.data)
            Protocol.send_message(client_socket.sendall, ack)

            while stream_state.not_trips_eof_received():
                self.__receive_client_message_and_publish(client_socket, stream_state, middleware)
            logging.info(f'action: receiving-metrics | status: in progress | process_id: {process_id}')
            metrics_obj = local_queue.get(block=True, timeout=None)
            logging.info(f'action: receiving-metrics | status: success | process_id: {process_id}')
            metrics_waiter.join()
            logging.info(f'action: sending metrics to client | status: in progress | process_id: {process_id}')
            Protocol.send_message(client_socket.sendall, metrics_obj)

            Loader.release_client_session(client_socket, static_ack_waiter, metrics_waiter)
        except Exception as e:
            logging.debug(f"Error: {e} | process_id: {process_id}", stack_info=True)
        finally:
            middleware.stop()

    @staticmethod
    def get_client_id():
        client_id = str(uuid.uuid4()) #16 bytes
        id_msg = Message.build_id_message(client_id)
        return id_msg

    def on_message_callback(self, message, delivery_tag):
        raise NotImplementedError

    def on_producer_finished(self, message, delivery_tag):
        raise NotImplementedError

    def on_eof_threshold_reached(self, eof_type: str, client_id: str, timestamp: str, stream_state: StreamState, middleware: LoaderMiddleware):
        if eof_type == STATIONS:
            replica_count = self._stations_consumer_replica_count
            send = middleware.send_stations
            stream_state.set_stations_eof()
        elif eof_type == WEATHER:
            replica_count = self._weather_consumer_replica_count
            send = middleware.send_weather
            stream_state.set_weather_eof()
        elif eof_type == TRIPS:
            replica_count = self._trips_consumer_replica_count
            send = middleware.send_trips
            stream_state.set_trips_eof()
        else:
            raise ValueError("Invalid type of data received")
        eof = Message.build_eof_message(message_type=eof_type, client_id=client_id, timestamp=timestamp)
        logging.info(f'sending {replica_count} eofs to: {eof_type}')
        for i in range(replica_count):
            send(Protocol.serialize_message(eof), i)

    def __receive_client_message_and_publish(self, client_socket, stream_state: StreamState, middleware: LoaderMiddleware):
        message = Protocol.receive_message(lambda n: recv_n_bytes(client_socket, n))
        message.timestamp = self.get_timestamp()
        if message.is_eof():
            self.on_eof_threshold_reached(message.message_type, message.client_id, message.timestamp, stream_state, middleware)
        else:
            module = None
            send_to = None
            if message.is_type(TRIPS):
                module = self._trips_consumer_replica_count
                send_to = middleware.send_trips
            elif message.is_type(STATIONS):
                module = self._stations_consumer_replica_count
                send_to = middleware.send_stations
            elif message.is_type(WEATHER):
                module = self._weather_consumer_replica_count
                send_to = middleware.send_weather
            else:
                raise ValueError("Invalid type of data received")

            routing_key = int(message.message_id) % module
            raw_msg = Protocol.serialize_message(message)
            send_to(raw_msg, routing_key)

    def _send_flush(self):
        flush_timestamp = self.get_timestamp()
        flush_message = Message.build_flush_message(flush_timestamp)
        flush_message = Protocol.serialize_message(flush_message)
        self._middleware.send_flush(flush_message)

    def get_timestamp(self):
        return str(time.time())

    @staticmethod
    def release_client_session(client_socket, static_ack_waiter, metrics_waiter):
        logging.info(f'action: close | status: in-progress | process_id: {os.getpid()}')
        if client_socket:
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
        # no necesitamos joinear acá porque ya joineamos anteriormente
        # lo unico que queremos es liberar los recursos
        metrics_waiter.close()
        static_ack_waiter.close()
