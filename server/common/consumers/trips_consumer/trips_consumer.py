from common.consumers.trips_consumer.trips_consumer_middleware import TripsConsumerMiddleware
from common.dag_node import DAGNode
from common_utils.protocol.message import Message, TRIPS, CLIENT_ID
from common_utils.protocol.protocol import Protocol
import logging


class TripsConsumer(DAGNode):
    filter_by_city_fields = {'client_id', 'message_id', 'start_station_code', 'end_station_code', 'yearid', 'city'}
    filter_by_year_fields = {'client_id', 'message_id', 'start_station_code', 'city', 'yearid'}
    join_by_date_fields = {'client_id', 'message_id', 'start_date', 'duration_sec', 'city'}

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

    def on_message_callback(self, message_obj: Message, _delivery_tag: int):
        if message_obj.is_eof():
            return
        filter_by_year_message = message_obj.pick_payload_fields(self.filter_by_year_fields)
        join_by_date_message = message_obj.pick_payload_fields(self.join_by_date_fields)
        filter_by_city_message = message_obj.pick_payload_fields(self.filter_by_city_fields)
        self.__send_message_to_filter_by_year(filter_by_year_message)
        self.__send_message_to_joiner_by_date(join_by_date_message)
        self.__send_message_to_filter_by_city(filter_by_city_message)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        eof = Message.build_eof_message(message_type=TRIPS, client_id=client_id)
        for _ in range(self._filter_by_year_consumers):
            self.__send_message_to_filter_by_year(eof)
        for _ in range(self._filter_by_city_consumers):
            self.__send_message_to_filter_by_city(eof)
        for _ in range(self._joiner_by_date_consumers):
            self.__send_message_to_joiner_by_date(eof)
        # self._middleware.stop()

    def __send_message_to_filter_by_city(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_filter_by_city_message(raw_message)

    def __send_message_to_filter_by_year(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_filter_by_year_message(raw_message)

    def __send_message_to_joiner_by_date(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_joiner_message(raw_message)

    def close(self):
        if not self.closed:
            super(TripsConsumer, self).close()
            self._middleware.stop()
