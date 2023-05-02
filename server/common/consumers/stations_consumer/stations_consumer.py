import logging

from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode
from common.utils import select_message_fields_decorator, message_from_payload_decorator
from typing import Dict, Union

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
STATIONS_QUEUE_NAME = 'stations'
JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE = 'join_by_year_city_station_id_stations'
JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE_TYPE = 'fanout'
FILTER_BY_CITY_ROUTING_KEY = 'filter_by_city_stations'


class StationsConsumer(DAGNode):
    filter_by_city_fields = ['code', 'yearid', 'city', 'name', 'latitude', 'longitude']
    joiner_by_year_city_station_id_fields = ['code', 'city', 'yearid', 'name']

    def __init__(self, rabbit_hostname: str,
                 filter_by_city_consumers: int = 1,
                 stations_producers: int = 1):
        super().__init__(rabbit_hostname)

        self._stations_queue = RabbitQueue(rabbit_connection=self._rabbit_connection,
                                           queue_name=STATIONS_QUEUE_NAME,
                                           bind_exchange=DATA_EXCHANGE,
                                           bind_exchange_type=DATA_EXCHANGE_TYPE,
                                           routing_key=STATIONS_QUEUE_NAME,
                                           producers=stations_producers)
        self._joiner_by_year_city_station_id_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
            exchange_name=JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE,
            exchange_type=JOINER_BY_YEAR_CITY_STATION_ID_EXCHANGE_TYPE,
        )
        self._filter_by_city_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )
        # Pub/Sub exchange, all consumers will get eof.
        self._joiner_consumers = 1
        self._filter_consumers = filter_by_city_consumers

    def run(self):
        try:
            self._stations_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            if self.closed:
                logging.info('action: shutdown | status: successful')
            else:
                raise e

    def on_message_callback(self, message_obj, _delivery_tag):
        payload = message_obj['payload']
        if payload == 'EOF':
            logging.info('eof received')
            return
        self.__send_message_to_filter_by_city(payload)
        self.__send_message_to_joiner_by_year_city_station_id(payload)

    def on_producer_finished(self, _message, delivery_tag):
        for _ in range(self._filter_consumers):
            self.__send_message_to_filter_by_city('EOF')
        self.__send_message_to_joiner_by_year_city_station_id('EOF')
        self.close()

    @select_message_fields_decorator(fields=filter_by_city_fields)
    @message_from_payload_decorator(message_type='stations')
    def __send_message_to_filter_by_city(self, message: Union[str, Dict]):
        self.publish(message, self._filter_by_city_exchange,
                     FILTER_BY_CITY_ROUTING_KEY)

    @select_message_fields_decorator(fields=joiner_by_year_city_station_id_fields)
    @message_from_payload_decorator(message_type='stations')
    def __send_message_to_joiner_by_year_city_station_id(self, message: Union[str, Dict]):
        self.publish(message, self._joiner_by_year_city_station_id_exchange)

