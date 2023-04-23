import socket
import logging
import json
import pika
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_exchange import RabbitExchange
from common_utils.utils import receive_string_message, recv_n_bytes
from common.dag_node import DAGNode


class Loader(DAGNode):
    def on_message_callback(self, message):
        raise NotImplementedError

    def __init__(self, port: int, backlog: int, rabbit_hostname: str, data_exchange: str, exchange_type: str):
        super().__init__()
        self._closed = False
        try:
            self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
            self._socket.bind(('', port))
            self._socket.listen(backlog)
            self._rabbit_connection = RabbitBlockingConnection(
                rabbit_hostname=rabbit_hostname
            )
            self._data_exchange = RabbitExchange(
                rabbit_connection=self._rabbit_connection,
                exchange_name=data_exchange,
                exchange_type=exchange_type,
            )
        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se
        except BaseException as e:
            logging.error(f'action: loader-init | status: failed | reason: {e}')
            raise e

    def run(self):
        logging.info('action: run | status: in progress')
        try:
            client_socket, _ = self._socket.accept()
        except socket.error as se:
            logging.error(f'action: socket-accept | status: failed | reason: {se}')
            raise se
        logging.info(f'action: socket-accept | status: success')
        while True:
            message = self.__receive_client_message(client_socket)
            json_message = json.loads(message)
            if isinstance(json_message['payload'], str):
                logging.debug(f'action: receive-client-message | status: success | message: {json_message}')
            self._data_exchange.publish(
                message,
                routing_key=json_message['type'],
            )

    def __receive_client_message(self, client_socket):
        return receive_string_message(recv_n_bytes, client_socket, 4)

    def close(self):
        if self._closed:
            return
        self._rabbit_connection.close()
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self._closed = True
        logging.debug('action: close-loader | status: success')
