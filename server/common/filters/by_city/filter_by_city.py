from common.filters.string_equality.string_equality import StringEquality

from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange


STATIONS_QUEUE_NAME = 'filter_by_city_stations'
TRIPS_QUEUE_NAME = 'filter_by_city_trips'
OUTPUT_EXCHANGE = 'filter_by_city'
OUTPUT_EXCHANGE_TYPE = 'direct'


class FilterByCity(StringEquality):
    def __init__(self,
                 filter_key: str,
                 filter_value: str,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 trips_producers: int = 1,
                 stations_producers: int = 1):
        super().__init__(filter_key, filter_value, rabbit_hostname, keep_filter_key)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=STATIONS_QUEUE_NAME,
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=TRIPS_QUEUE_NAME,
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
            exchange_name=OUTPUT_EXCHANGE,
            exchange_type=OUTPUT_EXCHANGE_TYPE,
        )

    def run(self):
        self._stations_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, _delivery_tag):
        to_send, message_obj = super(FilterByCity, self).on_message_callback(message, _delivery_tag)

    def on_producer_finished(self, message, delivery_tag):
        # TODO
        pass
