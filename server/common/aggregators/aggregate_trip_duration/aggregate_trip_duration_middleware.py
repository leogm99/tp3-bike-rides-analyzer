from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

ROUTING_KEY_PREFIX = 'aggregate_trip_duration'
METRICS_CONSUMER_ROUTING_KEY = 'metrics_consumer'


class AggregateTripDurationMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int, aggregate_id: int):
        super().__init__(hostname)
        self._node_id = aggregate_id
        self._routing_key = ROUTING_KEY_PREFIX + f'_{aggregate_id}'
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=self._routing_key,
            producers=producers
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

        self._delivery_tags = []

    def receive_trip_duration(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def send_metrics_message(self, message):
        self._output_exchange.publish(message, routing_key=METRICS_CONSUMER_ROUTING_KEY)

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)

    def ack_all(self):
        for delivery_tag in self._delivery_tags:
            self.ack_message(delivery_tag)
        self._delivery_tags = []

    def save_delivery_tag(self, delivery_tag):
        self._delivery_tags.append(delivery_tag)
        return len(self._delivery_tags) == self._input_queue.get_prefetch_count()

