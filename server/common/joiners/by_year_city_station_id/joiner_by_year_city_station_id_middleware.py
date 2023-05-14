from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

STATIONS_EXCHANGE = 'join_by_year_city_station_id_stations'
STATIONS_EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'joiner_by_year_city_station_id'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_COUNT_ROUTING_KEY = lambda n: f'aggregate_trip_count_{n}'


class JoinByYearCityStationIdMiddleware(Middleware):
    def __init__(self, hostname: str, stations_producers: int, trips_producers: int):
        super().__init__(hostname)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            bind_exchange=STATIONS_EXCHANGE,
            bind_exchange_type=STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )
        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=trips_producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection
        )

    def receive_stations(self, on_message_callback, on_end_message_callback):
        self._stations_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def cancel_consuming_stations(self):
        self._stations_input_queue.cancel()

    def send_static_data_ack(self, ack):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY)

    def send_aggregate_message(self, message, aggregate_id):
        self._output_exchange.publish(message, routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY(aggregate_id))
