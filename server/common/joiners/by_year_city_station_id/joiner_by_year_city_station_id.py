import json
import logging
from typing import Tuple

from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id_middleware import \
    JoinByYearCityStationIdMiddleware
from common.joiners.joiner import Joiner


class JoinByYearCityStationId(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 consumers: int = 1,
                 middleware: JoinByYearCityStationIdMiddleware = None):
        super().__init__(index_key)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_stations(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def join(self, payload):
        join_data = []
        for obj in payload:
            obj['code'] = obj.pop('start_station_code')
            data = super(JoinByYearCityStationId, self).join(obj)
            if data is not None:
                del data['city']
                del data['code']
                join_data.append(data)
        return join_data

    def on_message_callback(self, message_obj, delivery_tag):
        payload = message_obj['payload']
        if message_obj['type'] == 'stations':
            if message_obj['payload'] != 'EOF':
                self.insert_into_side_table(payload)
            return
        if payload == 'EOF':
            return
        join_data = self.join(payload)
        if join_data:
            hashes = self.hash_message(message=join_data, hashing_key='name', hash_modulo=self._consumers)
            for routing_key_suffix, obj in hashes.items():
                self._middleware.send_aggregate_message(json.dumps({'payload': obj}), routing_key_suffix)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            self._middleware.cancel_consuming_stations()
            self._middleware.send_static_data_ack(json.dumps({'type': 'notify', 'payload': 'ack'}))
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        if message['type'] == 'trips':
            logging.info('received EOS')
            for i in range(self._consumers):
                self._middleware.send_aggregate_message(json.dumps({'payload': 'EOF'}), i)
            self._middleware.stop()
