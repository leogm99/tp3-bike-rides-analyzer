import socket
import logging
import signal
import json

from common_utils.utils import receive_string_message, recv_n_bytes


class Loader:
    def __init__(self,
                 port: int,
                 backlog: int,
                 rabbit_hostname: str,
                 weather_queue: str,
                 stations_queue: str,
                 trips_queue: str,
                 static_data_ack_queue: str):
        signal.signal(signal.SIGTERM, lambda _n, _f: self.__close())
        self._closed = False
        try:
            self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
            self._socket.bind(('', port))
            self._socket.listen(backlog)
        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se

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

        self.__close()

    def __receive_client_message(self, client_socket):
        return receive_string_message(recv_n_bytes, client_socket, 4)

    def __close(self):
        if self._closed:
            return
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self.closed = True
        logging.debug('action: close-loader | status: success')
