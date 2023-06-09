from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_TRIPS = 'joiner_by_year_end_station_id'
STATIONS_QUEUE_NAME_PREFIX = 'joiner_by_year_end_station_id_stations'

FILTER_BY_CITY_STATIONS_EXCHANGE = 'filter_by_city_stations_output'
FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE = 'fanout'

STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
HAVERSINE_APPLIER_ROUTING_KEY = 'haversine_applier'


class JoinByYearEndStationIdMiddleware(Middleware):
    def __init__(self, hostname: str, stations_producers: int, trips_producers: int, node_id: int):
        super().__init__(hostname, prefetch_count=200)
        self._node_id = node_id
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{STATIONS_QUEUE_NAME_PREFIX}_{node_id}",
            bind_exchange=FILTER_BY_CITY_STATIONS_EXCHANGE,
            bind_exchange_type=FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{QUEUE_NAME_TRIPS}_{node_id}",
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
    
        self._stations_delivery_tags = []

    def receive_stations(self, on_message_callback, on_end_message_callback):
        self._stations_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self.ack_stations()
        self._stations_input_queue.flush(timestamp)
        self._trips_input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        ts = super().consume_flush(owner, callback)
        self._stations_input_queue.set_global_flush_timestamp(ts)
        self._trips_input_queue.set_global_flush_timestamp(ts)

    def send_static_data_ack(self, ack, client_id):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY + '_' + str(client_id))

    def send_haversine_message(self, message, routing_key):
        self._output_exchange.publish(message, routing_key=HAVERSINE_APPLIER_ROUTING_KEY + '_' + str(routing_key))

    def cancel_consuming_stations(self):
        self._stations_input_queue.cancel()

    def ack_trip(self, trip_delivery_tag):
        self._trips_input_queue.ack(trip_delivery_tag)

    def ack_stations(self):
        for delivery_tag in self._stations_delivery_tags:
            self._stations_input_queue.ack(delivery_tag)
        self._stations_delivery_tags = []

    def save_stations_delivery_tag(self, stations_delivery_tag):
        self._stations_delivery_tags.append(stations_delivery_tag)
        return len(self._stations_delivery_tags) == self._stations_input_queue.get_prefetch_count()
