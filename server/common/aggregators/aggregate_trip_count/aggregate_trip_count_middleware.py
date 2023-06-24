from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_PREFIX = lambda n: f'aggregate_trip_count_{n}'
FILTER_BY_COUNT_ROUTING_KEY = 'filter_by_count'


class AggregateTripCountMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int, aggregate_id: int):
        super().__init__(hostname)
        self._node_id = aggregate_id
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME_PREFIX(aggregate_id),
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

        self._delivery_tags = []

    def receive_trip_count(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self.ack_all()
        self._input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        super().consume_flush(owner, callback)
        ts = self.timestamp_store.get('timestamp')
        self._input_queue.set_global_flush_timestamp(ts)

    def send_filter_message(self, message, routing_key):
        self._output_exchange.publish(message, routing_key=f"{FILTER_BY_COUNT_ROUTING_KEY}_{routing_key}")

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)
    
    def ack_all(self):
        for delivery_tag in self._delivery_tags:
            self.ack_message(delivery_tag)
        self._delivery_tags = []

    def save_delivery_tag(self, delivery_tag):
        self._delivery_tags.append(delivery_tag)
        return len(self._delivery_tags) == self._input_queue.get_prefetch_count()