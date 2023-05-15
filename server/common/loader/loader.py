import socket
import logging
import queue

from common.loader.loader_middleware import LoaderMiddleware
from common.loader.metrics_waiter import MetricsWaiter
from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
from common_utils.utils import receive_string_message, recv_n_bytes, send_string_message
from common.dag_node import DAGNode
from common.loader.static_data_ack_waiter import StaticDataAckWaiter
from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware
from common_utils.protocol.message import Message, TRIPS, WEATHER, STATIONS
from common_utils.protocol.payload import Payload
from common_utils.protocol.protocol import Protocol


class Loader(DAGNode):
    def __init__(self,
                 port: int,
                 backlog: int,
                 stations_consumer_replica_count: int,
                 weather_consumer_replica_count: int,
                 trips_consumer_replica_count: int,
                 ack_count: int,
                 middleware: LoaderMiddleware,
                 static_data_ack_middleware: StaticDataAckWaiterMiddleware,
                 metrics_waiter_middleware: MetricsWaiterMiddleware):
        super().__init__()
        try:
            self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
            self._socket.bind(('', port))
            self._socket.listen(backlog)

            self._middleware = middleware

            self._stations_consumer_replica_count = stations_consumer_replica_count
            self._weather_consumer_replica_count = weather_consumer_replica_count
            self._trips_consumer_replica_count = trips_consumer_replica_count

            self._static_ack_waiter = StaticDataAckWaiter(
                ack_count,
                middleware=static_data_ack_middleware,
            )
            self._local_queue = queue.Queue()
            self._metrics_waiter = MetricsWaiter(
                local_queue=self._local_queue,
                middleware=metrics_waiter_middleware,
            )
            self._weather_eof = False
            self._stations_eof = False
            self._trips_eof = False
            self._client_sock = None

        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se
        except BaseException as e:
            logging.error(f'action: loader-init | status: failed | reason: {e}')
            raise e

    def run(self):
        try:
            self._static_ack_waiter.start()
            self._metrics_waiter.start()
            logging.info('action: run | status: in progress')
            client_socket, _ = self._socket.accept()
            self._client_sock = client_socket
            logging.info(f'action: socket-accept | status: success')
            while not self._weather_eof or not self._stations_eof:
                self.__receive_client_message_and_publish(self._client_sock)
            self._static_ack_waiter.join()
            ack = Message.build_ack_message()
            Protocol.send_message(self._client_sock.sendall, ack)
            while not self._trips_eof:
                self.__receive_client_message_and_publish(self._client_sock)
            logging.info('action: receiving-metrics | status: in progress')
            metrics_obj = self._local_queue.get(block=True, timeout=None)
            logging.info('action: receiving-metrics | status: success')
            self._metrics_waiter.join()
            logging.info('action: sending metrics to client | status: in progress')
            Protocol.send_message(self._client_sock.sendall, metrics_obj)
            self.close()
        except BrokenPipeError:
            logging.info('action: receive_client_message | connection closed by client')
        except BaseException as e:
            if not self.closed:
                raise e from e

    def on_message_callback(self, message, delivery_tag):
        raise NotImplementedError

    def on_producer_finished(self, message, delivery_tag):
        raise NotImplementedError

    def on_eof_threshold_reached(self, eof_type: str):
        if eof_type == STATIONS:
            replica_count = self._stations_consumer_replica_count
            send = self._middleware.send_stations
            self._stations_eof = True
        elif eof_type == WEATHER:
            replica_count = self._weather_consumer_replica_count
            send = self._middleware.send_weather
            self._weather_eof = True
        elif eof_type == TRIPS:
            replica_count = self._trips_consumer_replica_count
            send = self._middleware.send_trips
            self._trips_eof = True
        else:
            raise ValueError("Invalid type of data received")
        eof = Message.build_eof_message()
        for _ in range(replica_count):
            send(Protocol.serialize_message(eof))

    def __receive_client_message_and_publish(self, client_socket):
        message = Protocol.receive_message(lambda n: recv_n_bytes(client_socket, n))
        if message.is_eof():
            self.on_eof_threshold_reached(message.message_type)
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

    def close(self):
        if not self.closed:
            logging.info('action: close | status: in-progress')
            super(Loader, self).close()
            if self._client_sock:
                self._client_sock.shutdown(socket.SHUT_RDWR)
                self._client_sock.close()
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            if self._metrics_waiter.is_alive():
                self._metrics_waiter.close()
                self._metrics_waiter.join()
            if self._static_ack_waiter.is_alive():
                self._static_ack_waiter.close()
                self._static_ack_waiter.join()
