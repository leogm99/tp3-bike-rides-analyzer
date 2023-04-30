import abc
import signal
from typing import Union
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from hashlib import md5


class DAGNode(abc.ABC):
    def __init__(self, rabbit_hostname: str):
        self.__register_sigterm_handle()
        self.closed = False
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )

    @abc.abstractmethod
    def run(self):
        pass

    @staticmethod
    def publish(message: Union[str | bytes], exchange, routing_key=''):
        exchange.publish(
            message,
            routing_key=routing_key,
        )

    @abc.abstractmethod
    def on_message_callback(self, message, delivery_tag):
        pass

    @abc.abstractmethod
    def on_producer_finished(self, message, delivery_tag):
        pass

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()

    def __register_sigterm_handle(self):
        signal.signal(signal.SIGTERM, lambda *_: self.close())

    @staticmethod
    def hash_message(message, hashing_key: str, hash_modulo: int):
        if isinstance(message, list):
            buffers_hash = {k: [] for k in range(hash_modulo)}
            for obj in message:
                buffers_hash[int(md5(obj[hashing_key].encode()).hexdigest(), 16) % hash_modulo].append(obj)
            return buffers_hash
        else:
            return int(message[hashing_key]) % hash_modulo
