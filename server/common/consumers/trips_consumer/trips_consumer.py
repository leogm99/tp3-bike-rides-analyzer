from common.dag_node import DAGNode
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.utils import select_message_fields_decorator, message_from_payload_decorator
import logging

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
QUEUE_NAME = 'trips'
JOINER_BY_DATE = 'join_by_date'
FILTER_BY_YEAR_ROUTING_KEY = 'filter_by_year'
FILTER_BY_CITY_ROUTING_KEY = 'filter_by_city'


class TripsConsumer(DAGNode):
    filter_by_city_fields = ['start_station_code', 'end_station_code', 'city']
    filter_by_year_fields = ['start_station_code', 'city', 'yearid']
    join_by_date = ['start_date', 'duration_sec', 'city']

    def __init__(self,
                 rabbit_hostname: str,
                 trips_producers: int = 1,
                 filter_by_city_consumers: int = 1,
                 filter_by_year_consumers: int = 1):
        super().__init__(rabbit_hostname)

        self._trips_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=QUEUE_NAME,
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            producers=trips_producers,
            routing_key=QUEUE_NAME,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
        self._filter_by_city_consumers = filter_by_city_consumers
        self._filter_by_year_consumers = filter_by_year_consumers

    def run(self):
        try:
            self._trips_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            logging.info(f'error: {e}')

    def on_message_callback(self, message_obj, _delivery_tag):
        payload = message_obj['payload']
        if payload != 'EOF':
            self.__send_message_to_filter_by_year(payload)
            self.__send_message_to_joiner_by_date(payload)

    def on_producer_finished(self, _message, delivery_tag):
        for _ in range(self._filter_by_year_consumers):
            self.__send_message_to_filter_by_year('EOF')
        for _ in range(self._filter_by_city_consumers):
            self.__send_message_to_filter_by_city('EOF')

    @select_message_fields_decorator(fields=filter_by_city_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_filter_by_city(self, message: str):
        self.publish(message, self._output_exchange, FILTER_BY_CITY_ROUTING_KEY)

    @select_message_fields_decorator(fields=filter_by_year_fields)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_filter_by_year(self, message: str):
        self.publish(message, self._output_exchange, FILTER_BY_YEAR_ROUTING_KEY)

    @select_message_fields_decorator(fields=join_by_date)
    @message_from_payload_decorator(message_type='trips')
    def __send_message_to_joiner_by_date(self, message: str):
        self.publish(message, self._output_exchange, JOINER_BY_DATE)
