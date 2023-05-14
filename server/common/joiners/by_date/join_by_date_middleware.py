from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

WEATHER_EXCHANGE_NAME = 'join_by_date_weather'
WEATHER_EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'join_by_date'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_DURATION_ROUTING_KEY = lambda n: f'aggregate_trip_duration_{n}'


class JoinByDateMiddleware(Middleware):
    def __init__(self, hostname: str, weather_producers: int, trips_producers: int):
        super().__init__(hostname)
        self._weather_date_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='',
            bind_exchange=WEATHER_EXCHANGE_NAME,
            bind_exchange_type=WEATHER_EXCHANGE_TYPE,
            producers=weather_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_weather(self, on_message_callback, on_end_message_callback):
        self._weather_date_input_queue.consume(on_message_callback, on_end_message_callback)

    def receive_trips(self, on_message_callback, on_end_message_callback):
        self._trips_input_queue.consume(on_message_callback, on_end_message_callback)

    def send_aggregator_message(self, message, aggregator_id):
        self._output_exchange.publish(message, routing_key=AGGREGATE_TRIP_DURATION_ROUTING_KEY(aggregator_id))

    def send_static_data_ack(self, ack):
        self._output_exchange.publish(ack, routing_key=STATIC_DATA_ACK_ROUTING_KEY)

    def cancel_consuming_weather(self):
        self._weather_date_input_queue.cancel()
