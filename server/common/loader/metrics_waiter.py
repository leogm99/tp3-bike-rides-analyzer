from collections import defaultdict
import logging
from threading import Thread
from typing import Dict
from common_utils.KeyValueStore import KeyValueStore

from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
from common_utils.protocol.message import Message, METRICS
from common_utils.protocol.payload import Payload


class MetricsWaiter(Thread):
    def __init__(self, local_queue, middleware: MetricsWaiterMiddleware):
        super().__init__()
        self._middleware = middleware
        self._local_queue = local_queue
        self._closed = False
        self._metrics: KeyValueStore = KeyValueStore(defaultdict(list))

    def run(self) -> None:
        try:
            self._middleware.receive_metrics(self.__receive_metrics, self.__on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self._closed:
                raise e from e

    def __receive_metrics(self, message: Message, delivery_tag):
        self._metrics.append(message.message_type, message.payload.data)
        self._middleware.ack_metrics(delivery_tag)

    def __on_producer_finished(self, _message: Message, _delivery_tag):
        metrics_msg = Message(message_type=METRICS, payload=Payload(data=self._metrics.getAll()))
        self._local_queue.put(metrics_msg)
        logging.info('action: receive-metrics | status: success')
        return self.close

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._middleware.stop()
            except BaseException as e:
                pass
