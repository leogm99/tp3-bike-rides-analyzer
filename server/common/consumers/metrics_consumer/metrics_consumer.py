import logging

from common.consumers.metrics_consumer.metrics_consumer_middleware import MetricsConsumerMiddleware
from common.dag_node import DAGNode
from collections import defaultdict

from common_utils.protocol.message import Message, METRICS
from common_utils.protocol.payload import Payload
from common_utils.protocol.protocol import Protocol


class MetricsConsumer(DAGNode):
    def __init__(self,
                 middleware: MetricsConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware
        self._metrics = defaultdict(list)

    def run(self):
        try:
            self._middleware.receive_metrics(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        if message.is_eof():
            return
        self._metrics[message.message_type].append(message.payload.data)

    def on_producer_finished(self, message, delivery_tag):
        metrics = Message(message_type=METRICS, payload=Payload(data=self._metrics))
        raw_metrics = Protocol.serialize_message(metrics)
        self._middleware.send_metrics_message(raw_metrics)
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(MetricsConsumer, self).close()
            self._middleware.stop()

