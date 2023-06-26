from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'haversine_applier'
OUTPUT_ROUTING_KEY_PREFIX = lambda n: f'aggregate_trip_distance_{n}'


class HaversineApplierMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int, node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{QUEUE_NAME}_{node_id}",
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def send_aggregator_message(self, message, aggregator_id):
        self._output_exchange.publish(message, routing_key=OUTPUT_ROUTING_KEY_PREFIX(aggregator_id))

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self._input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        ts = super().consume_flush(owner, callback)
        self._input_queue.set_global_flush_timestamp(ts)
