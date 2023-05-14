import json
import logging
from typing import Tuple

from common.joiners.by_year_end_station_id.join_by_year_end_station_id_middleware import \
    JoinByYearEndStationIdMiddleware
from common.joiners.joiner import Joiner


class JoinByYearEndStationId(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 consumers: int = 1,
                 middleware: JoinByYearEndStationIdMiddleware = None):
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
        buffer = []
        for obj in payload:
            start_station_code = obj.pop('start_station_code')
            end_station_code = obj.pop('end_station_code')
            obj['code'] = start_station_code
            start_station_join = super(JoinByYearEndStationId, self).join(obj)
            obj['code'] = end_station_code
            end_station_join = super(JoinByYearEndStationId, self).join(obj)
            if start_station_join and end_station_join:
                start_station_join['start_station_latitude'] = start_station_join.pop('latitude')
                start_station_join['start_station_longitude'] = start_station_join.pop('longitude')
                end_station_join['end_station_latitude'] = end_station_join.pop('latitude')
                end_station_join['end_station_longitude'] = end_station_join.pop('longitude')
                end_station_join['end_station_name'] = end_station_join.pop('name')
                del start_station_join['name']
                del start_station_join['code']
                del start_station_join['yearid']
                del end_station_join['yearid']
                del end_station_join['code']
                result = start_station_join | end_station_join
                buffer.append(result)
        return buffer

    def on_message_callback(self, message, delivery_tag):
        if message['type'] == 'stations':
            if message['payload'] != 'EOF':
                self.insert_into_side_table(message['payload'])
            return
        payload = message['payload']
        if payload == 'EOF':
            return
        buffer = self.join(payload)
        if buffer:
            self.__send_message(json.dumps({'payload': buffer}))

    def __send_message(self, message):
        self._middleware.send_haversine_message(message)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            self._middleware.cancel_consuming_stations()
            self._middleware.send_static_data_ack(json.dumps({'type': 'notify', 'payload': 'ack'}))
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        elif message['type'] == 'trips':
            logging.info(f'action: on-producer-finished | received END OF STREAM: {message}')
            for _ in range(self._consumers):
                self._middleware.send_haversine_message(json.dumps({'payload': 'EOF'}))
            self._middleware.stop()

    def close(self):
        if not self.closed:
            super(JoinByYearEndStationId, self).close()
            self._middleware.stop()
