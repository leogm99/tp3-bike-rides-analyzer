import logging

from common.filters.string_equality.string_equality import StringEquality
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.utils import select_message_fields_decorator, message_from_payload_decorator

STATIONS_QUEUE_NAME = 'filter_by_city_stations'
TRIPS_QUEUE_NAME = 'filter_by_city_trips'
JOINER_BY_YEAR_ROUTING_KEY = 'joiner_by_year_end_station_id'
OUTPUT_EXCHANGE_STATIONS = 'filter_by_city_stations_output'
OUTPUT_EXCHANGE_STATIONS_TYPE = 'fanout'


class FilterByCity(StringEquality):
    stations_output_fields = ['code', 'yearid', 'name', 'latitude', 'longitude']
    trips_output_fields = ['start_station_code', 'end_station_code', 'yearid']

    def __init__(self,
                 filter_key: str,
                 filter_value: str,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 trips_producers: int = 1,
                 stations_producers: int = 1,
                 trips_consumers: int = 1):
        super().__init__(filter_key, filter_value, rabbit_hostname, keep_filter_key)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=STATIONS_QUEUE_NAME,
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=TRIPS_QUEUE_NAME,
            producers=trips_producers,
        )

        self._output_exchange_trips = RabbitExchange(
            self._rabbit_connection,
        )

        self._output_exchange_stations = RabbitExchange(
            self._rabbit_connection,
            exchange_name=OUTPUT_EXCHANGE_STATIONS,
            exchange_type=OUTPUT_EXCHANGE_STATIONS_TYPE,
        )
        # Pub/Sub exchange, everyone will get the EOF
        self._stations_consumers = 1
        self._trips_consumers = trips_consumers

    def run(self):
        try:
            self._stations_input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
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
        self.publish(message=message, exchange=self._output_exchange_stations)

    @select_message_fields_decorator(fields=trips_output_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_trips_message(self, message):
        self.publish(message=message, exchange=self._output_exchange_trips, routing_key=JOINER_BY_YEAR_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            logging.info('sending eof to station consumer')
            self.__send_stations_message('EOF')
            self._stations_input_queue.cancel()
        elif message['type'] == 'trips':
            for _ in range(self._trips_consumers):
                self.__send_trips_message('EOF')
            self.close()
