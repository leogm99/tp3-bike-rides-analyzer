from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

WEATHER_EXCHANGE_NAME = 'join_by_date_weather'
WEATHER_EXCHANGE_TYPE = 'fanout'
WEATHER_QUEUE_NAME_PREFIX = 'join_by_date_weather'
TRIPS_QUEUE_NAME = 'join_by_date'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_DURATION_ROUTING_KEY = lambda n: f'aggregate_trip_duration_{n}'


class JoinByDateMiddleware(Middleware):
    def __init__(self, hostname: str, weather_producers: int, trips_producers: int, node_id: int):
        super().__init__(hostname)
        self._node_id = node_id
        self._weather_date_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{WEATHER_QUEUE_NAME_PREFIX}_{node_id}",
            bind_exchange=WEATHER_EXCHANGE_NAME,
            bind_exchange_type=WEATHER_EXCHANGE_TYPE,
            producers=weather_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{TRIPS_QUEUE_NAME}_{node_id}",
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

        self._weather_delivery_tags = []

    def receive_weather(self, on_message_callback, on_end_message_callback):
        self._weather_date_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self.ack_weathers()
        self._weather_date_input_queue.flush(timestamp)
        self._trips_input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        super().consume_flush(owner, callback)
        ts = self.timestamp_store.get('timestamp')
        self._weather_date_input_queue.set_global_flush_timestamp(ts)
        self._trips_input_queue.set_global_flush_timestamp(ts)

    def send_aggregator_message(self, message, aggregator_id):
        self._output_exchange.publish(message, routing_key=AGGREGATE_TRIP_DURATION_ROUTING_KEY(aggregator_id))

    def send_static_data_ack(self, ack, client_id):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY + '_' + str(client_id))

    def cancel_consuming_weather(self):
        self._weather_date_input_queue.cancel()

    def ack_trip(self, trip_delivery_tag):
        self._trips_input_queue.ack(trip_delivery_tag)

    def ack_weathers(self):
        for delivery_tag in self._weather_delivery_tags:
            self._weather_date_input_queue.ack(delivery_tag)
        self._weather_delivery_tags = []

    def save_weather_delivery_tag(self, weather_delivery_tag):
        self._weather_delivery_tags.append(weather_delivery_tag)
        return len(self._weather_delivery_tags) == self._weather_date_input_queue.get_prefetch_count()
