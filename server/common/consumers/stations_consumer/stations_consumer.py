import logging

from common.consumers.stations_consumer.stations_consumer_middleware import StationsConsumerMiddleware
from common.dag_node import DAGNode
from common_utils.protocol.message import Message, STATIONS, CLIENT_ID
from common_utils.protocol.protocol import Protocol

ORIGIN_PREFIX = 'stations_consumer'

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

    def on_message_callback(self, message_obj: Message, delivery_tag):
        if message_obj.is_eof():
            return
        filter_by_city_message = message_obj.pick_payload_fields(self.filter_by_city_fields)
        joiner_by_year_city_station_id_message = message_obj.pick_payload_fields(
            self.joiner_by_year_city_station_id_fields)
        self.__send_message_to_filter_by_city(filter_by_city_message)
        self.__send_message_to_joiner_by_year_city_station_id(joiner_by_year_city_station_id_message)
        self._middleware.ack_message(delivery_tag)

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info('received eof')
        client_id = message.client_id
        eof = Message.build_eof_message(message_type=STATIONS,
                                        client_id=client_id,
                                        origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
        self.__send_message_to_filter_by_city(eof)
        self.__send_message_to_joiner_by_year_city_station_id(eof)
        

    def __send_message_to_filter_by_city(self, message: Message):
        if not message.is_eof():
            routing_key = int(message.message_id) % self._filter_consumers
            raw_msg = Protocol.serialize_message(message)
            self._middleware.send_filter_message(raw_msg, routing_key)
        else:
            raw_msg = Protocol.serialize_message(message)
            for i in range(self._filter_consumers):
                self._middleware.send_filter_message(raw_msg, i)

    def __send_message_to_joiner_by_year_city_station_id(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_joiner_message(raw_message)

    def close(self):
        if not self.closed:
            super(StationsConsumer, self).close()
            self._middleware.stop()
