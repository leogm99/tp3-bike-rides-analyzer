import logging
import json
from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.utils import select_message_fields, message_from_payload


class TripsConsumer(DAGNode):
    montreal_trips_filter_fields = ['start_station_code', 'end_station_code', 'city']
    trip_year_filter_fields = ['start_station_code', 'city', 'yearid']
    mean_trip_time_joiner_fields = ['start_date', 'duration_sec']

    def __init__(self,
                 rabbit_hostname: str,
                 data_exchange: str,
                 exchange_type: str,
                 trips_queue_name: str,
                 mean_trip_time_joiner_key: str,
                 trip_year_filter_routing_key: str,
                 montreal_trips_filter_routing_key: str):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )

        self._trips_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=trips_queue_name,
            bind_exchange=data_exchange,
            bind_exchange_type=exchange_type,
            routing_key=trips_queue_name,
        )

        self._montreal_trips_filter_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )
        self._trip_year_filter_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )
        self._mean_trip_time_joiner_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )

        self._montreal_trips_filter_routing_key = montreal_trips_filter_routing_key
        self._trip_year_filter_routing_key = trip_year_filter_routing_key
        self._mean_trip_time_joiner_key = mean_trip_time_joiner_key

    def run(self):
        try:
            self._trips_queue.consume(self.on_message_callback)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            if self.closed:
                pass

    def on_message_callback(self, body):
        obj_message = json.loads(body)
        payload = obj_message['payload']
        self.__send_message_to_montreal_trips_filter(payload)
        self.__send_message_to_trip_year_filter(payload)
        self.__send_message_to_mean_trip_time_joiner(payload)

    @select_message_fields(fields=montreal_trips_filter_fields)
    @message_from_payload(message_type='trips')
    def __send_message_to_montreal_trips_filter(self, message: str):
        self.publish(message, self._montreal_trips_filter_exchange, self._montreal_trips_filter_routing_key)

    @select_message_fields(fields=trip_year_filter_fields)
    @message_from_payload(message_type='trips')
    def __send_message_to_trip_year_filter(self, message: str):
        self.publish(message, self._trip_year_filter_exchange, self._trip_year_filter_routing_key)

    @select_message_fields(fields=mean_trip_time_joiner_fields)
    @message_from_payload(message_type='trips')
    def __send_message_to_mean_trip_time_joiner(self, message: str):
        self.publish(message, self._mean_trip_time_joiner_exchange, self._mean_trip_time_joiner_key)

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
