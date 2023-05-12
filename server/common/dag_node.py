import abc
import logging
import signal
from hashlib import md5


class DAGNode(abc.ABC):
    def __init__(self):
        self.__register_sigterm_handle()
        self.closed = False

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def on_message_callback(self, message, delivery_tag):
        pass

    @abc.abstractmethod
    def on_producer_finished(self, message, delivery_tag):
        pass

    def close(self):
        logging.info('action: close-dag-node')
        if not self.closed:
            self.closed = True

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
            return int(md5(message[hashing_key].encode()).hexdigest(), 16) % hash_modulo
