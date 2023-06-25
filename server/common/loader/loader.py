import socket
import logging
import threading
import time
import queue

from common.loader.client_handler import ClientHandler
from common.loader.loader_middleware import LoaderMiddleware
from common.dag_node import DAGNode
from common_utils.protocol.message import Message
from common_utils.protocol.protocol import Protocol


def get_timestamp():
    return time.time()


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

            self._client_socket_queue = queue.Queue()
            self._middleware: LoaderMiddleware = self._middleware_callback()
            self._middleware.create_flush_channel()
            self.__send_flush()

            self._client_handlers = []
            self._client_semaphore = threading.BoundedSemaphore(value=self._clients_number)
            self._stop_event = threading.Event()
            self.__launch_client_handlers()

        except socket.error as se:
            logging.error(f'action: socket-create | status: failed | reason: {se}')
            raise se
        except BaseException as e:
            logging.error(f'action: loader-init | status: failed | reason: {e}')
            raise e

    def run(self):
        try:
            self.__accept_clients()
        except socket.error as e:
            logging.info(f'action: accept-clients | status: stopped')
        except BaseException as e:
            logging.error(f'action: run | error: {e}')

    def close(self):
        logging.info('action: loader-close | status: in progress')
        if not self.closed:
            self._middleware.stop()
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            logging.debug('action: loader-close | stopping client handlers')
            # in case some handler is waiting in the queue...
            for _ in range(len(self._client_handlers)):
                self._client_socket_queue.put_nowait(None)
            for ch in self._client_handlers:
                ch.stop()
                ch.join()

            logging.debug('action: loader-close | stopped client_handlers')
            super(Loader, self).close()
        logging.info('action: loader-close | status: successful')

    def __launch_client_handlers(self):
        for _ in range(self._clients_number):
            client_handler = ClientHandler(
                hostname=self._hostname,
                client_socket_queue=self._client_socket_queue,
                client_semaphore=self._client_semaphore,
                middleware_factory=self._middleware_callback,
                trips_consumer_replica_count=self._trips_consumer_replica_count,
                weather_consumer_replica_count=self._weather_consumer_replica_count,
                stations_consumer_replica_count=self._weather_consumer_replica_count,
                ack_count=self._ack_count,
                stop_event=self._stop_event,
            )
            client_handler.start()
            self._client_handlers.append(client_handler)

    def __accept_clients(self):
        self._open_server_socket()
        while True:
            logging.info('action: Accepting clients | status: in progress')
            client_socket, _ = self._socket.accept()
            # should not throw exception as the queue has no upper bound
            self._client_socket_queue.put_nowait(client_socket)
            self._client_semaphore.acquire()

    def _open_server_socket(self):
        self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
        self._socket.bind(('', self._port))
        self._socket.listen(self._backlog)

    def on_message_callback(self, message, delivery_tag):
        raise NotImplementedError

    def on_producer_finished(self, message, delivery_tag):
        raise NotImplementedError

    def __send_flush(self):
        flush_timestamp = get_timestamp()
        flush_message = Message.build_flush_message(flush_timestamp)
        flush_message = Protocol.serialize_message(flush_message)
        self._middleware.send_flush(flush_message)
