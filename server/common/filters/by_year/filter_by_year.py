import json
import logging

from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_year'
JOINER_BY_YEAR_CITY_STATION_ID_ROUTING_KEY = 'joiner_by_year_city_station_id'


class FilterByYear(NumericRange):
    def __init__(self,
                 filter_key: str,
                 low: float,
                 high: float,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 producers: int = 1,
                 consumers: int = 1):
        super().__init__(filter_key, low, high, rabbit_hostname, keep_filter_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
        self._consumers = consumers

    def run(self):
        try:
            self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, _delivery_tag):
        if message['payload'] == 'EOF':
            return
        to_send, message_obj = super(FilterByYear, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self._output_exchange.publish(json.dumps(message_obj),
                                          routing_key=JOINER_BY_YEAR_CITY_STATION_ID_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        logging.info('action: on-producer-finished | received EOS')
        for _ in range(self._consumers):
            self.publish(json.dumps({'type': 'trips', 'payload': 'EOF'}), self._output_exchange,
                         routing_key=JOINER_BY_YEAR_CITY_STATION_ID_ROUTING_KEY)
        self.close()
