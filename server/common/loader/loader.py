import multiprocessing
import os
import socket
import logging
import queue
import uuid
from multiprocessing.pool import ThreadPool

from common.loader.loader_middleware import LoaderMiddleware
from common.loader.metrics_waiter import MetricsWaiter
from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
from common_utils.utils import receive_string_message, recv_n_bytes, send_string_message
from common.dag_node import DAGNode
from common.loader.static_data_ack_waiter import StaticDataAckWaiter
from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware
from common_utils.protocol.message import Message, TRIPS, WEATHER, STATIONS, CLIENT_ID
from common_utils.protocol.payload import Payload
from common_utils.protocol.protocol import Protocol
from common.loader.client_manager import ClientManager
from common.loader.stream_state import StreamState

class Loader(DAGNode):
    def __init__(self,
                 port: int,
                 backlog: int,
                 stations_consumer_replica_count: int,
                 weather_consumer_replica_count: int,
                 trips_consumer_replica_count: int,
                 ack_count: int,
                 middleware: LoaderMiddleware,
                 hostname: str,
                 max_clients: int):
        super().__init__()
        try:
            self._socket = None
            self._port = port
            self._backlog = backlog

            self._middleware = middleware

            self._stations_consumer_replica_count = stations_consumer_replica_count
            self._weather_consumer_replica_count = weather_consumer_replica_count
            self._trips_consumer_replica_count = trips_consumer_replica_count

            self._ack_count = ack_count
            self._hostname = hostname
            self._clients_number = max_clients

            self._process_queue = multiprocessing.Queue()
            self._queue_lock = multiprocessing.Lock()
            self._client_manager: ClientManager = ClientManager(self._clients_number)

        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se
        except BaseException as e:
            logging.error(f'action: loader-init | status: failed | reason: {e}')
            raise e

    def run(self):
        try:
            #with multiprocessing.Pool(processes=self._clients_number) as pool:
            #    pool.map(self.process_loop, range(self._clients_number))
            # TypeError: cannot pickle '_thread.lock' object
            for _ in range(self._clients_number):
                p = multiprocessing.Process(target=self.process_loop)
                p.start()

            self.accept_clients()
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

    def accept_clients(self):
        while True:
            self._open_server_socket()
            logging.info('action: Accepting clients | status: in progress')
            while True:
                client_socket, _ = self._socket.accept()
                self._process_queue.put(client_socket)
                if not self._client_manager.add_client():
                    logging.info('action: Max connections reached')
                    break
            self._socket.close()
            self._client_manager.wait_slot_available()

    def _open_server_socket(self):
        self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
        self._socket.bind(('', self._port))
        self._socket.listen(self._backlog)

    def process_loop(self):
        while True:
            with self._queue_lock:
                client_socket = self._process_queue.get(block=True, timeout=None)
            
            self._run(client_socket, self._hostname, self._ack_count)
            self._client_manager.remove_client()

    def _run(self, client_socket, hostname, ack_count):
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

            streamState = StreamState()

            while streamState.is_static_data_eof():
                self.__receive_client_message_and_publish(client_socket, streamState)
            static_ack_waiter.join()
            ack = Message.build_ack_message(client_id.payload.data)
            Protocol.send_message(client_socket.sendall, ack)

            while streamState.is_trips_eof():
                self.__receive_client_message_and_publish(client_socket, streamState)
            logging.info(f'action: receiving-metrics | status: in progress | process_id: {process_id}')
            metrics_obj = local_queue.get(block=True, timeout=None)
            logging.info(f'action: receiving-metrics | status: success | process_id: {process_id}')
            metrics_waiter.join()
            logging.info(f'action: sending metrics to client | status: in progress | process_id: {process_id}')
            Protocol.send_message(client_socket.sendall, metrics_obj)

            self.close(client_socket, static_ack_waiter, metrics_waiter)
        except Exception as e:
            logging.info(f"Error: {e} | process_id: {process_id}")

    def get_client_id(self):
        id = str(uuid.uuid4()) #16 bytes
        id_msg = Message.build_id_message(id)
        return id_msg

    def on_message_callback(self, message, delivery_tag):
        raise NotImplementedError

    def on_producer_finished(self, message, delivery_tag):
        raise NotImplementedError

    def on_eof_threshold_reached(self, eof_type: str, client_id: str, streamState: StreamState):
        if eof_type == STATIONS:
            replica_count = self._stations_consumer_replica_count
            send = self._middleware.send_stations
            streamState.set_stations_eof()
        elif eof_type == WEATHER:
            replica_count = self._weather_consumer_replica_count
            send = self._middleware.send_weather
            streamState.set_weather_eof()
        elif eof_type == TRIPS:
            replica_count = self._trips_consumer_replica_count
            send = self._middleware.send_trips
            streamState.set_trips_eof()
        else:
            raise ValueError("Invalid type of data received")
        eof = Message.build_eof_message(client_id=client_id)
        for _ in range(replica_count):
            send(Protocol.serialize_message(eof))

    def __receive_client_message_and_publish(self, client_socket, streamState: StreamState):
        message = Protocol.receive_message(lambda n: recv_n_bytes(client_socket, n))
        if message.is_eof():
            self.on_eof_threshold_reached(message.message_type, message.payload.data[CLIENT_ID], streamState)
        else:
            raw_message = Protocol.serialize_message(message)
            if message.is_type(TRIPS):
                self._middleware.send_trips(raw_message)
            elif message.is_type(STATIONS):
                self._middleware.send_stations(raw_message)
            elif message.is_type(WEATHER):
                self._middleware.send_weather(raw_message)
            else:
                raise ValueError("Invalid type of data received")

    def close(self, client_socket, static_ack_waiter, metrics_waiter):
        logging.info(f'action: close | status: in-progress | process_id: {os.getpid()}')
        if client_socket:
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
        if metrics_waiter.is_alive():
            metrics_waiter.close()
            metrics_waiter.join()
        if static_ack_waiter.is_alive():
            static_ack_waiter.close()
            static_ack_waiter.join()
