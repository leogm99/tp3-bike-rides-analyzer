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
    def __init__(self, hostname: str, producers: int, node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{QUEUE_NAME}_{node_id}",
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=f"{QUEUE_NAME}_{node_id}",
            producers=producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_trips(self, on_message_callback, on_end_message_callback):
        super().receive(self._input_queue, on_message_callback, on_end_message_callback, auto_ack=False)

    def send_joiner_message(self, message, routing_key_postfix):
        super().send(message, self._output_exchange, f"{JOINER_BY_DATE}_{routing_key_postfix}")

    def send_filter_by_city_message(self, message, routing_key_postfix):
        super().send(message, self._output_exchange, f"{FILTER_BY_CITY_ROUTING_KEY}_{routing_key_postfix}")

    def send_filter_by_year_message(self, message, routing_key_postfix):
        super().send(message, self._output_exchange, f"{FILTER_BY_YEAR_ROUTING_KEY}_{routing_key_postfix}")

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)
