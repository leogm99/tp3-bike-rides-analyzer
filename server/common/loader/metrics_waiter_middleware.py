from common.middleware.middleware import Middleware
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'metrics_waiter'


class MetricsWaiterMiddleware(Middleware):
    def __init__(self, hostname: str, client_id: str):
        super().__init__(hostname)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME + '_' + client_id,
        )

    def receive_metrics(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)
    
    def ack_metrics(self, delivery_tag):
        self._input_queue.ack(delivery_tag)
