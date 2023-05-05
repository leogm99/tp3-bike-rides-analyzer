import socket
import logging
import json
import queue

from common.loader.metrics_waiter import MetricsWaiter
from common.rabbit.rabbit_exchange import RabbitExchange
from common_utils.utils import receive_string_message, recv_n_bytes, send_string_message
from common.dag_node import DAGNode
from common.loader.static_data_ack_waiter import StaticDataAckWaiter

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'


class Loader(DAGNode):
    def __init__(self,
                 port: int,
                 backlog: int,
                 rabbit_hostname: str,
                 stations_consumer_replica_count: int,
                 weather_consumer_replica_count: int,
                 trips_consumer_replica_count: int,
                 ack_count: int):
        super().__init__(rabbit_hostname)
        try:
            self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
            self._socket.bind(('', port))
            self._socket.listen(backlog)
            # Obs: capa de negocio conoce al middleware
            self._data_exchange = RabbitExchange(
                rabbit_connection=self._rabbit_connection,
                exchange_name=DATA_EXCHANGE,
                exchange_type=DATA_EXCHANGE_TYPE,
            )
            self._stations_consumer_replica_count = stations_consumer_replica_count
            self._weather_consumer_replica_count = weather_consumer_replica_count
            self._trips_consumer_replica_count = trips_consumer_replica_count

            self._static_ack_waiter = StaticDataAckWaiter(
                rabbit_hostname,
                ack_count
            )
            self._local_queue = queue.Queue()
            self._metrics_waiter = MetricsWaiter(
                rabbit_hostname=rabbit_hostname,
                local_queue=self._local_queue
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
            ack = json.dumps({'type': 'ack'})
            send_string_message(self._client_sock.sendall, ack, 4)
            while not self._trips_eof:
                self.__receive_client_message_and_publish(self._client_sock)
            logging.info('action: receiving-metrics | status: in progress')
            metrics_obj = self._local_queue.get(block=True, timeout=None)
            logging.info('action: receiving-metrics | status: success')
            self._metrics_waiter.join()
            logging.info('action: sending metrics to client | status: in progress')
            send_string_message(self._client_sock.sendall, json.dumps(metrics_obj), 4)
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
        if eof_type == 'stations':
            replica_count = self._stations_consumer_replica_count
            self._stations_eof = True
        elif eof_type == 'weather':
            replica_count = self._weather_consumer_replica_count
            self._weather_eof = True
        elif eof_type == 'trips':
            replica_count = self._trips_consumer_replica_count
            self._trips_eof = True
        else:
            raise ValueError("Invalid type of data received")
        eof = {'type': eof_type, 'payload': 'EOF'}
        for _ in range(replica_count):
            self.publish(json.dumps(eof),
                         exchange=self._data_exchange,
                         routing_key=eof_type)

    def __receive_client_message_and_publish(self, client_socket):
        message = receive_string_message(recv_n_bytes, client_socket, 4)
        json_message = json.loads(message)
        if json_message['payload'] == 'EOF':
            self.on_eof_threshold_reached(json_message['type'])
        else:
            self._data_exchange.publish(
                message,
                routing_key=json_message['type'],
            )

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
