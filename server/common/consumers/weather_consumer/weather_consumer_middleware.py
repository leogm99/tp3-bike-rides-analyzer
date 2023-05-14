from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
WEATHER_QUEUE_NAME = 'weather'
FILTER_BY_PRECIPITATION_ROUTING_KEY = 'filter_by_precipitation'


class WeatherConsumerMiddleware(Middleware):
    def __init__(self, hostname: str, producers: int):
        super().__init__(hostname)
        self._weather_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=WEATHER_QUEUE_NAME,
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=WEATHER_QUEUE_NAME,
            producers=producers,
        )

        self._output_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )

    def receive_weather(self, on_message_callback, on_end_message_callback):
        self._weather_queue.consume(on_message_callback, on_end_message_callback)

    def send_to_filter(self, message):
        self._output_exchange.publish(message, routing_key=FILTER_BY_PRECIPITATION_ROUTING_KEY)
