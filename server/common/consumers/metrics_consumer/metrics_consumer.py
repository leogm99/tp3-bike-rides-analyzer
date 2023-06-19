import logging
from typing import Dict

from common.consumers.metrics_consumer.metrics_consumer_middleware import MetricsConsumerMiddleware
from common.dag_node import DAGNode
from collections import defaultdict

from common_utils.protocol.message import Message, METRICS, CLIENT_ID
from common_utils.protocol.payload import Payload
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore


class MetricsConsumer(DAGNode):
    def __init__(self,
                 middleware: MetricsConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware
        self._metrics_by_client_id: Dict[str, KeyValueStore] = {}

    def run(self):
        try:
            self._middleware.receive_metrics(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message: Message, delivery_tag):
        if message.is_eof():
            return
        client_id = self._get_client_id(message)
        self._metrics_by_client_id[client_id].append(message.message_type, message.payload.data)
        self._middleware.ack_message(delivery_tag)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = self._get_client_id(message)
        logging.info(f'sending metrics! {self._metrics_by_client_id[client_id].getAll()}')
        metrics = Message(message_type=METRICS, payload=Payload(data=self._metrics_by_client_id[client_id].getAll()))
        raw_metrics = Protocol.serialize_message(metrics)
        self._middleware.send_metrics_message(raw_metrics, client_id)
        

    def close(self):
        if not self.closed:
            super(MetricsConsumer, self).close()
            self._middleware.stop()

    def _get_client_id(self, message: Message):
        client_id = message.client_id
        if client_id not in self._metrics_by_client_id:
            self._metrics_by_client_id[client_id] = KeyValueStore(defaultdict(list))
        return client_id
