from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

STATIONS_EXCHANGE = 'join_by_year_city_station_id_stations'
STATIONS_EXCHANGE_TYPE = 'fanout'
STATIONS_QUEUE_NAME_PREFIX = 'join_by_year_city_station_id_stations'
TRIPS_QUEUE_NAME = 'joiner_by_year_city_station_id'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_COUNT_ROUTING_KEY = lambda n: f'aggregate_trip_count_{n}'


class JoinByYearCityStationIdMiddleware(Middleware):
    def __init__(self, 
                 hostname: str, 
                 stations_producers: int, 
                 trips_producers: int, 
                 node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{STATIONS_QUEUE_NAME_PREFIX}_{node_id}",
            bind_exchange=STATIONS_EXCHANGE,
            bind_exchange_type=STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )
        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{TRIPS_QUEUE_NAME}_{node_id}",
            producers=trips_producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection
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

    def cancel_consuming_stations(self):
        self._stations_input_queue.cancel()

    def send_static_data_ack(self, ack, client_id):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY + '_' + str(client_id))

    def send_aggregate_message(self, message, aggregate_id):
        self._output_exchange.publish(message, routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY(aggregate_id))

    def ack_trip(self, trip_delivery_tag):
        self._trips_input_queue.ack(trip_delivery_tag)

    def ack_stations(self):
        for delivery_tag in self._stations_delivery_tags:
            self._stations_input_queue.ack(delivery_tag)
        self._stations_delivery_tags = []

    def save_stations_delivery_tag(self, stations_delivery_tag):
        self._stations_delivery_tags.append(stations_delivery_tag)
        return len(self._stations_delivery_tags) == self._stations_input_queue.get_prefetch_count()
