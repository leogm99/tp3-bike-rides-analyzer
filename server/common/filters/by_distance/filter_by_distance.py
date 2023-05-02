import json
import logging

from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_distance'
METRICS_CONSUMER_ROUTING_KEY = 'metrics_consumer'


class FilterByDistance(NumericRange):
    def __init__(self, filter_key: str,
                 low: float,
                 high: float,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 producers: int = 1):
        super().__init__(filter_key, low, high, rabbit_hostname, keep_filter_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, _delivery_tag):
        if message['payload'] == 'EOF':
            return
        to_send, message_obj = super(FilterByDistance, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self.publish(json.dumps(message_obj), self._output_exchange,
                         routing_key=METRICS_CONSUMER_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        self.publish(json.dumps({'type': 'distance_metric', 'payload': 'EOF'}), self._output_exchange,
                     routing_key=METRICS_CONSUMER_ROUTING_KEY)
        self.close()
