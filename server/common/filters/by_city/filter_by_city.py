import logging

from common.filters.by_city.filter_by_city_middleware import FilterByCityMiddleware
from common.filters.string_equality.string_equality import StringEquality
from common.utils import select_message_fields_decorator, message_from_payload_decorator


class FilterByCity(StringEquality):
    stations_output_fields = ['code', 'yearid', 'name', 'latitude', 'longitude']
    trips_output_fields = ['start_station_code', 'end_station_code', 'yearid']

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
        if message['payload'] == 'EOF':
            return
        to_send, message_obj = super(FilterByCity, self).on_message_callback(message, _delivery_tag)
        if to_send:
            if message['type'] == 'stations':
                self.__send_stations_message(message_obj['payload'])
            elif message['type'] == 'trips':
                self.__send_trips_message(message_obj['payload'])

    @select_message_fields_decorator(fields=stations_output_fields)
    @message_from_payload_decorator(message_type='stations')
    def __send_stations_message(self, message):
        self._middleware.send_stations_message(message)

    @select_message_fields_decorator(fields=trips_output_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_trips_message(self, message):
        self._middleware.send_trips_message(message)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            logging.info('sending eof to station consumer')
            self.__send_stations_message('EOF')
            self._middleware.cancel_consuming_stations()
        elif message['type'] == 'trips':
            for _ in range(self._trips_consumers):
                self.__send_trips_message('EOF')
            self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByCity, self).close()
            self._middleware.stop()
