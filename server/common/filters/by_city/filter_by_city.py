import logging

from common.filters.by_city.filter_by_city_middleware import FilterByCityMiddleware
from common.filters.string_equality.string_equality import StringEquality
from common_utils.protocol.message import Message, TRIPS, STATIONS, CLIENT_ID
from common_utils.protocol.protocol import Protocol


class FilterByCity(StringEquality):
    stations_output_fields = {'client_id', 'message_id', 'code', 'yearid', 'name', 'latitude', 'longitude'}
    trips_output_fields = {'client_id', 'message_id', 'start_station_code', 'end_station_code', 'yearid'}

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
            self._middleware.receive_stations(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, _delivery_tag):
        if message.is_eof():
            return
        to_send, message_obj = super(FilterByCity, self).on_message_callback(message, _delivery_tag)
        if to_send:
            if message.is_type(STATIONS):
                message_obj = message_obj.pick_payload_fields(self.stations_output_fields)
                self.__send_stations_message(message_obj)
            elif message.is_type(TRIPS):
                message_obj = message_obj.pick_payload_fields(self.trips_output_fields)
                self.__send_trips_message(message_obj)

    def __send_stations_message(self, message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_stations_message(raw_message)

    def __send_trips_message(self, message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_trips_message(raw_message)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        if message.is_type(STATIONS):
            stations_eof = Message.build_eof_message(message_type=STATIONS, client_id=client_id)
            self.__send_stations_message(stations_eof)
            self._middleware.cancel_consuming_stations()
        elif message.is_type(TRIPS):
            trips_eof = Message.build_eof_message(message_type=TRIPS, client_id=client_id)
            for _ in range(self._trips_consumers):
                self.__send_trips_message(trips_eof)
            # self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByCity, self).close()
            self._middleware.stop()
