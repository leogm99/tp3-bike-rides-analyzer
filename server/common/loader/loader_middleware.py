from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
TRIPS_KEY = 'trips'
STATIONS_KEY = 'stations'
WEATHER_KEY = 'weather'


class LoaderMiddleware(Middleware):
    def __init__(self, hostname: str):
        super().__init__(hostname)
        self._data_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
            exchange_name=DATA_EXCHANGE,
            exchange_type=DATA_EXCHANGE_TYPE,
        )

    def send_trips(self, message, routing_key_postfix):
        self._data_exchange.publish(message, routing_key=f"{TRIPS_KEY}_{routing_key_postfix}")

    def send_stations(self, message, routing_key_postfix):
        self._data_exchange.publish(message, routing_key=f"{STATIONS_KEY}_{routing_key_postfix}")

    def send_weather(self, message, routing_key_postfix):
        self._data_exchange.publish(message, routing_key=f"{WEATHER_KEY}_{routing_key_postfix}")
