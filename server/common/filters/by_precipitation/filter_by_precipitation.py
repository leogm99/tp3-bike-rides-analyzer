import json
import logging

from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange

QUEUE_NAME = 'filter_by_precipitation'
OUTPUT_EXCHANGE_NAME = 'join_by_date_weather'
OUTPUT_EXCHANGE_TYPE = 'fanout'


class FilterByPrecipitation(NumericRange):
    def __init__(self,
                 filter_key: str,
                 low: float,
                 high: float,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 weather_producers: int = 1):
        super().__init__(filter_key, low, high, rabbit_hostname, keep_filter_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=weather_producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
            exchange_name=OUTPUT_EXCHANGE_NAME,
            exchange_type=OUTPUT_EXCHANGE_TYPE,
        )
        # Pub/Sub exchange, everyone will get the EOF message
        self._weather_consumers = 1

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, _delivery_tag):
        to_send, message_obj = super(FilterByPrecipitation, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self._output_exchange.publish(json.dumps(message_obj))

    def on_producer_finished(self, message, delivery_tag):
        eof = {'type': 'weather', 'payload': 'EOF'}
        logging.info('sending eof')
        for _ in range(self._weather_consumers):
            self._output_exchange.publish(json.dumps(eof))
        self.close()

