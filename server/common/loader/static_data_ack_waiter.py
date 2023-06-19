from threading import Thread

from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware
from common_utils.protocol.message import Message

QUEUE_NAME = 'static_data_ack'


class StaticDataAckWaiter(Thread):
    def __init__(self, needed_ack: int, middleware: StaticDataAckWaiterMiddleware):
        super().__init__()
        self._middleware = middleware
        self._needed_ack = needed_ack
        self._ack_count = 0
        self._closed = False
        # No need to log, if loader fails, flush everything
        self._origins = set()

    def run(self) -> None:
        try:
            self._middleware.receive_ack(self.ack_receiver, lambda *_: None)
            self._middleware.start()
        except BaseException as e:
            if not self._closed:
                raise e from e

    def ack_receiver(self, message: Message, delivery_tag):
        self._middleware.ack_static_data_acknowledgment(delivery_tag)
        if message.origin not in self._origins:
            self._origins.add(message.origin) 
            if self._needed_ack == len(self._origins):
                return self.close

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._middleware.stop()
            except BaseException as e:
                pass
