import logging

from common.consumers.stations_consumer.stations_consumer_middleware import StationsConsumerMiddleware
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

    def __init__(self, filter_by_city_consumers: int = 1,
                 middleware: StationsConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware

        # Pub/Sub exchange, all consumers will get eof.
        self._joiner_consumers = 1
        self._filter_consumers = filter_by_city_consumers

    def run(self):
        try:
            self._middleware.receive_stations(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

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
        self._middleware.stop()

    @select_message_fields_decorator(fields=filter_by_city_fields)
    @message_from_payload_decorator(message_type='stations')
    def __send_message_to_filter_by_city(self, message: Union[str, Dict]):
        self._middleware.send_filter_message(message)

    @select_message_fields_decorator(fields=joiner_by_year_city_station_id_fields)
    @message_from_payload_decorator(message_type='stations')
    def __send_message_to_joiner_by_year_city_station_id(self, message: Union[str, Dict]):
        self._middleware.send_joiner_message(message)

