import abc
import signal
from typing import Union


class DAGNode(abc.ABC):
    def __init__(self):
        self.__register_sigterm_handle()
        self.closed = False

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
    def on_message_callback(self, message):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def __register_sigterm_handle(self):
        signal.signal(signal.SIGTERM, lambda *_: self.close())
