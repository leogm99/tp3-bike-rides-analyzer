from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
WEATHER_QUEUE_NAME = 'weather'
FILTER_BY_PRECIPITATION_ROUTING_KEY = 'filter_by_precipitation'


class WeatherConsumerMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int, node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._weather_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=f"{WEATHER_QUEUE_NAME}_{node_id}",
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=f"{WEATHER_QUEUE_NAME}_{node_id}",
            producers=producers,
        )

        self._output_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )

    def receive_weather(self, on_message_callback, on_end_message_callback):
        self._weather_queue.consume(on_message_callback, on_end_message_callback)

    def send_to_filter(self, message, routing_key_postfix):
        self._output_exchange.publish(message, routing_key=f"{FILTER_BY_PRECIPITATION_ROUTING_KEY}_{routing_key_postfix}")

    def ack_message(self, delivery_tag):
        self._weather_queue.ack(delivery_tag)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self._weather_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        super().consume_flush(owner, callback)
        ts = self.timestamp_store.get('timestamp')
        self._weather_queue.set_global_flush_timestamp(ts)
