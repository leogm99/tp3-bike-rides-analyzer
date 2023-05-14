from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_TRIPS = 'joiner_by_year_end_station_id'

FILTER_BY_CITY_STATIONS_EXCHANGE = 'filter_by_city_stations_output'
FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE = 'fanout'

STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
HAVERSINE_APPLIER_ROUTING_KEY = 'haversine_applier'


class JoinByYearEndStationIdMiddleware(Middleware):
    def __init__(self, hostname: str, stations_producers: int, trips_producers: int):
        super().__init__(hostname)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            bind_exchange=FILTER_BY_CITY_STATIONS_EXCHANGE,
            bind_exchange_type=FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME_TRIPS,
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_stations(self, on_message_callback, on_end_message_callback):
        self._stations_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def send_static_data_ack(self, ack):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY)

    def send_haversine_message(self, message):
        self._output_exchange.publish(message, routing_key=HAVERSINE_APPLIER_ROUTING_KEY)

    def cancel_consuming_stations(self):
        self._stations_input_queue.cancel()
