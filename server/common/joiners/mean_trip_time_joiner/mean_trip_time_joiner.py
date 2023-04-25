import json
import logging

from common.dag_node import DAGNode

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue


class MeanTripTimeJoiner(DAGNode):
    def __init__(self, rabbit_hostname):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )
        self._weather_input_queue = RabbitQueue(
            self._rabbit_connection,
            bind_exchange='mean_trip_time_joiner_weather',
            bind_exchange_type='fanout'
        )
        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='mean_trip_time_joiner_trips',
        )

    def run(self):
        self._weather_input_queue.consume(self.on_message_callback)
        self._trips_input_queue.consume(self.on_message_callback)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message):
        message_obj = json.loads(message)
        payload = message_obj['payload']
        # logging.info(f'message: {message_obj}')
        pass

    def close(self):
        pass

