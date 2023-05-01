import logging
import json

from haversine import haversine

from common.appliers.applier import Applier
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue
from common.utils import select_message_fields_decorator

QUEUE_NAME = 'haversine_applier'
OUTPUT_ROUTING_KEY = 'aggregate_trip_distance'


class HaversineApplier(Applier):
    output_fields = ['end_station_name', 'distance']

    def __init__(self,
                 rabbit_hostname: str,
                 start_latitude_key: str,
                 start_longitude_key: str,
                 end_latitude_key: str,
                 end_longitude_key: str):
        super().__init__(rabbit_hostname)
        self._start_latitude_key = start_latitude_key
        self._start_longitude_key = start_longitude_key
        self._end_latitude_key = end_latitude_key
        self._end_longitude_key = end_longitude_key

        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if isinstance(payload, list):
            for obj in payload:
                distance_calculated = self.apply(obj)
                obj['distance'] = distance_calculated
            self.__send_message(payload)

    def on_producer_finished(self, message, delivery_tag):
        pass

    @select_message_fields_decorator(fields=output_fields)
    def __send_message(self, message):
        self.publish(json.dumps({'payload': message}), self._output_exchange, routing_key=OUTPUT_ROUTING_KEY)

    def apply(self, message):
        start = float(message[self._start_latitude_key]), float(message[self._start_longitude_key])
        end = float(message[self._end_latitude_key]), float(message[self._end_longitude_key])
        return haversine(start, end)
