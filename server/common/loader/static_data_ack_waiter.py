from threading import Thread

from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware

QUEUE_NAME = 'static_data_ack'


class StaticDataAckWaiter(Thread):
    def __init__(self, needed_ack: int, middleware: StaticDataAckWaiterMiddleware):
        super().__init__()
        self._middleware = middleware
        self._needed_ack = needed_ack
        self._ack_count = 0
        self._closed = False

    def run(self) -> None:
        try:
            self._middleware.receive_ack(self.ack_receiver, lambda *_: None)
            self._middleware.start()
        except BaseException as e:
            if not self._closed:
                raise e from e

    def ack_receiver(self, _message, _delivery_tag):
        self._ack_count += 1
        if self._needed_ack == self._ack_count:
            self.close()

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._middleware.stop()
            except BaseException as e:
                pass
