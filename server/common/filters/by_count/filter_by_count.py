import json
import logging

from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_count'
METRICS_CONSUMER_ROUTING_KEY = 'metrics_consumer'


class FilterByCount(NumericRange):
    def __init__(self,
                 filter_key: str,
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
        try:
            self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message_obj, delivery_tag):
        if message_obj['payload'] == 'EOF':
            return
        # filter data that has no counts the previous year
        if message_obj['payload']['year_2016'] == 0:
            return
        # trips_2017 > 2*trips_2016 <-> trips_2017/2 > trips_2016
        self.high = float(message_obj['payload']['year_2017']) / 2
        to_send, obj = super(FilterByCount, self).on_message_callback(message_obj, delivery_tag)
        if to_send:
            self.publish(json.dumps({'type': 'count_metric', 'payload': obj['payload']}), self._output_exchange,
                         routing_key=METRICS_CONSUMER_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        self.publish(json.dumps({'type': 'count_metric', 'payload': 'EOF'}), self._output_exchange,
                     routing_key=METRICS_CONSUMER_ROUTING_KEY)
        self.close()
