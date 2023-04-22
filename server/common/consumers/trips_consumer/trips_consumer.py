import logging
import json
from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange


class TripsConsumer(DAGNode):
    def __init__(self,
                 rabbit_hostname: str,
                 data_exchange: str,
                 exchange_type: str,
                 trips_queue_name: str,
                 mean_trip_time_joiner_exchange_name: str,
                 mean_trip_time_joiner_exchange_type: str,
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
            exchange_name=mean_trip_time_joiner_exchange_name,
            exchange_type=mean_trip_time_joiner_exchange_type,
        )

        self._montreal_trips_filter_routing_key = montreal_trips_filter_routing_key
        self._trip_year_filter_routing_key = trip_year_filter_routing_key

        self._montreal_trips_filter_fields = ['start_station_code', 'end_station_code', 'city']
        self._trip_year_filter_fields = ['start_station_code', 'city', 'yearid']
        self._mean_trip_time_joiner_fields = ['start_date', 'duration_sec']

    def run(self):
        try:
            self._trips_queue.consume(self.__on_message_callback)
        except BaseException as e:
            if self.closed:
                pass

    def __on_message_callback(self, ch, method, properties, body):
        try:
            obj_message = json.loads(body)
            if obj_message['payload'] == 'EOF':
                # TODO
                return
            montreal_trips_filter_payload = self.select_dictionary_fields(obj_message['payload'],
                                                                          self._montreal_trips_filter_fields)
            trip_year_filter_payload = self.select_dictionary_fields(obj_message['payload'],
                                                                     self._trip_year_filter_fields)
            mean_trip_time_joiner_payload = self.select_dictionary_fields(obj_message['payload'],
                                                                          self._mean_trip_time_joiner_fields)
            obj_message['payload'] = montreal_trips_filter_payload
            self._montreal_trips_filter_exchange.publish(
                json.dumps(obj_message),
                routing_key=self._montreal_trips_filter_routing_key,
            )
            obj_message['payload'] = trip_year_filter_payload
            self._trip_year_filter_exchange.publish(
                json.dumps(obj_message),
                routing_key=self._trip_year_filter_routing_key,
            )
            obj_message['payload'] = mean_trip_time_joiner_payload
            self._mean_trip_time_joiner_exchange.publish(
                json.dumps(obj_message)
            )
        except BaseException as e:
            logging.error(f'action: on-message-callback | status: failed | error: {e}')
            raise e

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
