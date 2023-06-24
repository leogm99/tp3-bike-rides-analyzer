from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_precipitation'
OUTPUT_EXCHANGE_NAME = 'join_by_date_weather'
OUTPUT_EXCHANGE_TYPE = 'fanout'


class FilterByPrecipitationMiddleware(Middleware):
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
            exchange_name=OUTPUT_EXCHANGE_NAME,
            exchange_type=OUTPUT_EXCHANGE_TYPE,
        )

    def receive_weather(self, on_message_callback, on_end_message_callback):
        self._input_queue.consume(on_message_callback, on_end_message_callback)

    def send_joiner_message(self, message):
        self._output_exchange.publish(message)

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self._input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        super().consume_flush(owner, callback)
        ts = self.timestamp_store.get('timestamp')
        self._input_queue.set_global_flush_timestamp(ts)
