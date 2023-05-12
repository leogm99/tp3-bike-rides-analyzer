import logging
import json

from haversine import haversine

from common.appliers.applier import Applier
from common.appliers.haversine_applier.haversine_applier_middleware import HaversineApplierMiddleware
from common.utils import select_message_fields_decorator


class HaversineApplier(Applier):
    output_fields = ['end_station_name', 'distance']

    def __init__(self,
                 start_latitude_key: str,
                 start_longitude_key: str,
                 end_latitude_key: str,
                 end_longitude_key: str,
                 consumers: int = 1,
                 middleware: HaversineApplierMiddleware = None):
        super().__init__()
        self._middleware = middleware
        self._start_latitude_key = start_latitude_key
        self._start_longitude_key = start_longitude_key
        self._end_latitude_key = end_latitude_key
        self._end_longitude_key = end_longitude_key

        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        for obj in payload:
            distance_calculated = self.apply(obj)
            obj['distance'] = distance_calculated
        self.__send_message(payload)

    def on_producer_finished(self, message, delivery_tag):
        for i in range(self._consumers):
            self._middleware.send_aggregator_message(
                json.dumps({'payload': 'EOF'}), i
            )
        self._middleware.stop()

    @select_message_fields_decorator(fields=output_fields)
    def __send_message(self, message):
        hashes = self.hash_message(message, hashing_key='end_station_name', hash_modulo=self._consumers)
        for routing_key, buffer in hashes.items():
            self._middleware.send_aggregator_message(json.dumps({'payload': buffer}), routing_key)

    def apply(self, message):
        start = float(message[self._start_latitude_key]), float(message[self._start_longitude_key])
        end = float(message[self._end_latitude_key]), float(message[self._end_longitude_key])
        return haversine(start, end)
