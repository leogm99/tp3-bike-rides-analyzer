from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_count'
METRICS_CONSUMER_ROUTING_KEY = 'metrics_consumer'


class FilterByCountMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int, node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{QUEUE_NAME}_{node_id}",
            producers=producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_count_aggregate(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def send_metrics_message(self, message):
        self._output_exchange.publish(message, routing_key=METRICS_CONSUMER_ROUTING_KEY)
