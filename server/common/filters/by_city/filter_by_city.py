import logging

from common.filters.by_city.filter_by_city_middleware import FilterByCityMiddleware
from common.filters.string_equality.string_equality import StringEquality
from common_utils.protocol.message import Message, TRIPS, STATIONS, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol

ORIGIN_PREFIX = 'filter_by_city'

class FilterByCity(StringEquality):
    stations_output_fields = {'code', 'yearid', 'name', 'latitude', 'longitude'}
    trips_output_fields = {'start_station_code', 'end_station_code', 'yearid'}

    def __init__(self,
                 filter_key: str,
                 filter_value: str,
                 keep_filter_key: bool = False,
                 trips_consumers: int = 1,
                 middleware: FilterByCityMiddleware = None):
        super().__init__(filter_key, filter_value, keep_filter_key)
        self._middleware = middleware
        # Pub/Sub exchange, everyone will get the EOF
        self._stations_consumers = 1
        self._trips_consumers = trips_consumers

    def run(self):
        try:
            self._middleware.consume_flush(f"{FLUSH}_{ORIGIN_PREFIX}_{self._middleware._node_id}", self.on_flush)
            self._middleware.receive_stations(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        if message.is_eof():
            return
        to_send, message_obj = super(FilterByCity, self).on_message_callback(message, delivery_tag)
        if to_send:
            if message.is_type(STATIONS):
                message_obj = message_obj.pick_payload_fields(self.stations_output_fields)
                self.__send_stations_message(message_obj)
            elif message.is_type(TRIPS):
                message_obj = message_obj.pick_payload_fields(self.trips_output_fields)
                self.__send_trips_message(message_obj)
        if message.is_type(STATIONS):
            self._middleware.ack_stations_message(delivery_tag)
        elif message.is_type(TRIPS):
            self._middleware.ack_trips_message(delivery_tag)

    def __send_stations_message(self, message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_stations_message(raw_message)

    def __send_trips_message(self, message: Message):
        if not message.is_eof():
            routing_key = int(message.message_id) % self._trips_consumers
            raw_msg = Protocol.serialize_message(message)
            self._middleware.send_trips_message(raw_msg, routing_key)
        else:
            raw_msg = Protocol.serialize_message(message)
            for i in range(self._trips_consumers):
                self._middleware.send_trips_message(raw_msg, i)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        timestamp = message.timestamp
        if message.is_type(STATIONS):
            stations_eof = Message.build_eof_message(message_type=STATIONS, client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            self.__send_stations_message(stations_eof)
        elif message.is_type(TRIPS):
            trips_eof = Message.build_eof_message(message_type=TRIPS, client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            self.__send_trips_message(trips_eof)
            
    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)

    def close(self):
        if not self.closed:
            super(FilterByCity, self).close()
            self._middleware.stop()
