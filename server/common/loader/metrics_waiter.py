import logging
from threading import Thread

from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware


class MetricsWaiter(Thread):
    def __init__(self, local_queue, middleware: MetricsWaiterMiddleware):
        super().__init__()
        self._middleware = middleware
        self._local_queue = local_queue
        self._closed = False

    def run(self) -> None:
        try:
            self._middleware.receive_metrics(self.__receive_metrics, lambda: None)
            self._middleware.start()
        except BaseException as e:
            if not self._closed:
                raise e from e

    def __receive_metrics(self, message, _delivery_tag):
        logging.info('action: receive-metrics | status: in progress')
        self._local_queue.put(message)
        logging.info('action: receive-metrics | status: success')
        self.close()

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._middleware.stop()
            except BaseException as e:
                pass
