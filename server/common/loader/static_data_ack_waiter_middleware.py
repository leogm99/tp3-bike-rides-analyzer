from common.middleware.middleware import Middleware
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'static_data_ack'


class StaticDataAckWaiterMiddleware(Middleware):
    def __init__(self, hostname: str):
        super().__init__(hostname)
        self._static_data_ack = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
        )

    def receive_ack(self, on_message_callback, on_end_message_callback):
        self._static_data_ack.consume(on_message_callback, on_end_message_callback)
