import json
import logging
from typing import Tuple
from datetime import datetime, timedelta

from common.joiners.by_date.join_by_date_middleware import JoinByDateMiddleware
from common.joiners.joiner import Joiner

DELTA_CORRECTION = timedelta(days=1)


class JoinByDate(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 consumers: int = 1,
                 middleware: JoinByDateMiddleware = None):
        super().__init__(index_key)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_weather(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def join(self, payload):
        join_data = []
        for obj in payload:
            obj['date'] = obj.pop('start_date')
            trip_date = datetime.strptime(obj['date'], '%Y-%m-%d %H:%M:%S')
            obj['date'] = datetime.strftime(trip_date, '%Y-%m-%d')
            data = super(JoinByDate, self).join(obj)
            if data is not None:
                del data['city']
                join_data.append(data)
        return join_data

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if message['type'] == 'weather':
            if payload == 'EOF':
                return
            for obj in payload:
                weather_date = datetime.strptime(obj['date'], "%Y-%m-%d") - DELTA_CORRECTION
                obj['date'] = weather_date.strftime('%Y-%m-%d')
            self.insert_into_side_table(payload, save_key='date')
            return
        try:
            if payload == 'EOF':
                return
            join_data = self.join(payload)
            if join_data:
                hashes = self.hash_message(message=join_data, hashing_key='date', hash_modulo=self._consumers)
                for routing_key_postfix, message_buffer in hashes.items():
                    self._middleware.send_aggregator_message(json.dumps({'payload': message_buffer}),
                                                             routing_key_postfix)
        except BaseException as e:
            logging.info(f'error: {e}')

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'weather':
            self._middleware.cancel_consuming_weather()
            self._middleware.send_static_data_ack(json.dumps({'type': 'notify', 'payload': 'ack'}))
        if message['type'] == 'trips':
            for i in range(self._consumers):
                self._middleware.send_aggregator_message(json.dumps({'payload': 'EOF'}), i)
            self._middleware.stop()
