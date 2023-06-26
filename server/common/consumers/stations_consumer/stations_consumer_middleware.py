from common.middleware.middleware import Middleware
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
STATIONS_QUEUE_NAME = 'stations'
JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE = 'join_by_year_city_station_id_stations'
JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE_TYPE = 'fanout'
FILTER_BY_CITY_ROUTING_KEY = 'filter_by_city_stations'


class StationsConsumerMiddleware(Middleware):
    def __init__(self, hostname, producers, node_id: int):
        super(StationsConsumerMiddleware, self).__init__(hostname)
        self._node_id = node_id
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=f"{STATIONS_QUEUE_NAME}_{node_id}",
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=f"{STATIONS_QUEUE_NAME}_{node_id}",
            producers=producers,
        )

        self._joiner_exchange = RabbitExchange(
            self._rabbit_connection,
            exchange_name=JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE,
            exchange_type=JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE_TYPE,
        )

        self._filter_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def receive_stations(self, on_message_callback, on_end_message_callback):
        super().receive(self._input_queue, on_message_callback, on_end_message_callback, auto_ack=False)

    def send_joiner_message(self, message):
        super().send(message, self._joiner_exchange)

    def send_filter_message(self, message, routing_key_postfix):
        super().send(message, self._filter_exchange, f"{FILTER_BY_CITY_ROUTING_KEY}_{routing_key_postfix}")

    def ack_message(self, delivery_tag):
        self._input_queue.ack(delivery_tag)

    def flush(self, timestamp):
        self.timestamp_store['timestamp'] = timestamp
        self.timestamp_store.dumps('timestamp_store.json')
        self._input_queue.flush(timestamp)

    def consume_flush(self, owner, callback):
        ts = super().consume_flush(owner, callback)
        self._input_queue.set_global_flush_timestamp(ts)
