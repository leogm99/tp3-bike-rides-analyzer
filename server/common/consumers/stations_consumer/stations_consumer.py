import logging

from common.consumers.stations_consumer.stations_consumer_middleware import StationsConsumerMiddleware
from common.dag_node import DAGNode
from common_utils.protocol.message import Message, STATIONS
from common_utils.protocol.protocol import Protocol


class StationsConsumer(DAGNode):
    filter_by_city_fields = {'code', 'yearid', 'city', 'name', 'latitude', 'longitude'}
    joiner_by_year_city_station_id_fields = {'code', 'city', 'yearid', 'name'}

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

    def on_message_callback(self, message_obj: Message, _delivery_tag):
        if message_obj.is_eof():
            return
        filter_by_city_message = message_obj.pick_payload_fields(self.filter_by_city_fields)
        joiner_by_year_city_station_id_message = message_obj.pick_payload_fields(
            self.joiner_by_year_city_station_id_fields)
        self.__send_message_to_filter_by_city(filter_by_city_message)
        self.__send_message_to_joiner_by_year_city_station_id(joiner_by_year_city_station_id_message)

    def on_producer_finished(self, _message, delivery_tag):
        logging.info('received eof')
        eof = Message.build_eof_message(message_type=STATIONS)
        for _ in range(self._filter_consumers):
            self.__send_message_to_filter_by_city(eof)
        self.__send_message_to_joiner_by_year_city_station_id(eof)
        self._middleware.stop()

    def __send_message_to_filter_by_city(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_filter_message(raw_message)

    def __send_message_to_joiner_by_year_city_station_id(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_joiner_message(raw_message)

    def close(self):
        if not self.closed:
            super(StationsConsumer, self).close()
            self._middleware.stop()
