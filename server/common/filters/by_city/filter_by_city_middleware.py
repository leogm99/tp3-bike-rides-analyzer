from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

STATIONS_QUEUE_NAME = 'filter_by_city_stations'
TRIPS_QUEUE_NAME = 'filter_by_city_trips'
JOINER_BY_YEAR_ROUTING_KEY = 'joiner_by_year_end_station_id'
OUTPUT_EXCHANGE_STATIONS = 'filter_by_city_stations_output'
OUTPUT_EXCHANGE_STATIONS_TYPE = 'fanout'


class FilterByCityMiddleware(Middleware):
    def __init__(self, hostname: str, stations_producers: int, trips_producers: int, node_id):
        super().__init__(hostname)
        self._node_id = node_id
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{STATIONS_QUEUE_NAME}_{node_id}",
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{TRIPS_QUEUE_NAME}_{node_id}",
            producers=trips_producers,
        )

        self._output_exchange_trips = RabbitExchange(
            self._rabbit_connection,
        )

        self._output_exchange_stations = RabbitExchange(
            self._rabbit_connection,
            exchange_name=OUTPUT_EXCHANGE_STATIONS,
            exchange_type=OUTPUT_EXCHANGE_STATIONS_TYPE,
        )

    def receive_stations(self, on_message_callback, on_end_message_callback):
        self._stations_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def send_stations_message(self, message):
        self._output_exchange_stations.publish(message)

    def send_trips_message(self, message, routing_key_postfix):
        self._output_exchange_trips.publish(message, routing_key=f"{JOINER_BY_YEAR_ROUTING_KEY}_{routing_key_postfix}")

    def cancel_consuming_stations(self):
        self._stations_input_queue.cancel()
