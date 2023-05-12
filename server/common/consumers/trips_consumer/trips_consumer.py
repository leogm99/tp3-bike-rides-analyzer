from common.consumers.trips_consumer.trips_consumer_middleware import TripsConsumerMiddleware
from common.dag_node import DAGNode
from common.utils import select_message_fields_decorator, message_from_payload_decorator
import logging


class TripsConsumer(DAGNode):
    filter_by_city_fields = ['start_station_code', 'end_station_code', 'yearid', 'city']
    filter_by_year_fields = ['start_station_code', 'city', 'yearid']
    join_by_date = ['start_date', 'duration_sec', 'city']

    def __init__(self,
                 filter_by_city_consumers: int = 1,
                 filter_by_year_consumers: int = 1,
                 joiner_by_date_consumers: int = 1,
                 middleware: TripsConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware
        self._filter_by_city_consumers = filter_by_city_consumers
        self._filter_by_year_consumers = filter_by_year_consumers
        self._joiner_by_date_consumers = joiner_by_date_consumers

    def run(self):
        try:
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message_obj, _delivery_tag):
        payload = message_obj['payload']
        if payload != 'EOF':
            self.__send_message_to_filter_by_year(payload)
            self.__send_message_to_joiner_by_date(payload)
            self.__send_message_to_filter_by_city(payload)

    def on_producer_finished(self, _message, delivery_tag):
        for _ in range(self._filter_by_year_consumers):
            self.__send_message_to_filter_by_year('EOF')
        for _ in range(self._filter_by_city_consumers):
            self.__send_message_to_filter_by_city('EOF')
        for _ in range(self._joiner_by_date_consumers):
            self.__send_message_to_joiner_by_date('EOF')
        self._middleware.stop()

    @select_message_fields_decorator(fields=filter_by_city_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_filter_by_city(self, message: str):
        self._middleware.send_filter_by_city_message(message)

    @select_message_fields_decorator(fields=filter_by_year_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_filter_by_year(self, message: str):
        self._middleware.send_filter_by_year_message(message)

    @select_message_fields_decorator(fields=join_by_date)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_joiner_by_date(self, message: str):
        self._middleware.send_joiner_message(message)
