import logging
from threading import Thread

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue


class MetricsWaiter(Thread):
    def __init__(self, rabbit_hostname: str, local_queue):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname
        )
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='metrics_waiter',
        )
        self._local_queue = local_queue
        self._closed = False

    def run(self) -> None:
        try:
            self._input_queue.consume(self.__receive_metrics, lambda: None)
            self._rabbit_connection.start_consuming()
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
                self._rabbit_connection.close()
            except BaseException as e:
                pass
