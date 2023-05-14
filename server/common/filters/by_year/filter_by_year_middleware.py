from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_year'
JOINER_BY_YEAR_CITY_STATION_ID_ROUTING_KEY = 'joiner_by_year_city_station_id'


class FilterByYearMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int):
        super().__init__(hostname)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def send_joiner_message(self, message):
        self._output_exchange.publish(message, routing_key=JOINER_BY_YEAR_CITY_STATION_ID_ROUTING_KEY)
