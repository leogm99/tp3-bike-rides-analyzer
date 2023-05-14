from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
QUEUE_NAME = 'trips'
JOINER_BY_DATE = 'join_by_date'
FILTER_BY_YEAR_ROUTING_KEY = 'filter_by_year'
FILTER_BY_CITY_ROUTING_KEY = 'filter_by_city_trips'


class TripsConsumerMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int):
        super().__init__(hostname)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=QUEUE_NAME,
            producers=producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_trips(self, on_message_callback, on_end_message_callback):
        super().receive(self._input_queue, on_message_callback, on_end_message_callback, auto_ack=False)

    def send_joiner_message(self, message):
        super().send(message, self._output_exchange, JOINER_BY_DATE)

    def send_filter_by_city_message(self, message):
        super().send(message, self._output_exchange, FILTER_BY_CITY_ROUTING_KEY)

    def send_filter_by_year_message(self, message):
        super().send(message, self._output_exchange, FILTER_BY_YEAR_ROUTING_KEY)
