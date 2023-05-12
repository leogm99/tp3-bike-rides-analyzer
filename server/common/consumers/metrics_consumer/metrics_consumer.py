import json
import logging

from common.consumers.metrics_consumer.metrics_consumer_middleware import MetricsConsumerMiddleware
from common.dag_node import DAGNode
from collections import defaultdict


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
        if message['payload'] == 'EOF':
            return
        self._metrics[message['type']].append(message['payload'])

    def on_producer_finished(self, message, delivery_tag):
        self._middleware.send_metrics_message(json.dumps(self._metrics))
        self._middleware.stop()

