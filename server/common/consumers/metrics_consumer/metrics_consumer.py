import logging

from common.consumers.metrics_consumer.metrics_consumer_middleware import MetricsConsumerMiddleware
from common.dag_node import DAGNode

from common_utils.protocol.message import Message, METRICS, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol


class MetricsConsumer(DAGNode):
    def __init__(self,
                 middleware: MetricsConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.consume_flush(f"{FLUSH}_metrics", self.on_flush)
            self._middleware.receive_metrics(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message: Message, delivery_tag):
        if message.is_eof():
            return
        client_id = message.client_id
        raw_metric = Protocol.serialize_message(message)
        self._middleware.send_metrics_message(raw_metric, client_id)
        self._middleware.ack_message(delivery_tag)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        timestamp = message.timestamp
        logging.info(f'sending EOF for metrics! for client_id: {client_id}')
        eof = Message.build_eof_message(message_type=METRICS, client_id=client_id, timestamp=timestamp)
        raw_msg = Protocol.serialize_message(eof)
        self._middleware.send_metrics_message(raw_msg, client_id)

    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)

    def close(self):
        if not self.closed:
            super(MetricsConsumer, self).close()
            self._middleware.stop()
