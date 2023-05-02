import logging
import json

from haversine import haversine

from common.appliers.applier import Applier
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue
from common.utils import select_message_fields_decorator

QUEUE_NAME = 'haversine_applier'
OUTPUT_ROUTING_KEY_PREFIX = lambda n: f'aggregate_trip_distance_{n}'


class HaversineApplier(Applier):
    output_fields = ['end_station_name', 'distance']

    def __init__(self,
                 rabbit_hostname: str,
                 start_latitude_key: str,
                 start_longitude_key: str,
                 end_latitude_key: str,
                 end_longitude_key: str,
                 producers: int = 1,
                 consumers: int = 1):
        super().__init__(rabbit_hostname)
        self._start_latitude_key = start_latitude_key
        self._start_longitude_key = start_longitude_key
        self._end_latitude_key = end_latitude_key
        self._end_longitude_key = end_longitude_key

        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
        self._consumers = consumers

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
        for i in range(self._consumers):
            self.publish(json.dumps({'payload': 'EOF'}), self._output_exchange,
                         routing_key=OUTPUT_ROUTING_KEY_PREFIX(i))
        self.close()

    @select_message_fields_decorator(fields=output_fields)
    def __send_message(self, message):
        hashes = self.hash_message(message, hashing_key='end_station_name', hash_modulo=self._consumers)
        for routing_key, buffer in hashes.items():
            self.publish(json.dumps({'payload': buffer}), self._output_exchange,
                         routing_key=OUTPUT_ROUTING_KEY_PREFIX(routing_key))

    def apply(self, message):
        start = float(message[self._start_latitude_key]), float(message[self._start_longitude_key])
        end = float(message[self._end_latitude_key]), float(message[self._end_longitude_key])
        return haversine(start, end)
