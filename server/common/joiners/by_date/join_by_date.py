import json
import logging
from typing import Tuple
from datetime import datetime, timedelta

from common.joiners.by_date.join_by_date_middleware import JoinByDateMiddleware
from common.joiners.joiner import Joiner
from common_utils.protocol.message import Message, TRIPS, WEATHER, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol

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
            obj.data['date'] = obj.data.pop('start_date')
            trip_date = datetime.strptime(obj.data['date'], '%Y-%m-%d %H:%M:%S')
            obj.data['date'] = datetime.strftime(trip_date, '%Y-%m-%d')
            join_payload = super(JoinByDate, self).join(obj)
            if join_payload is not None:
                del join_payload.data['city']
                join_data.append(join_payload)
        return join_data

    def on_message_callback(self, message: Message, delivery_tag):
        if message.is_eof():
            return
        if message.is_type(WEATHER):
            for obj in message.payload:
                weather_date = datetime.strptime(obj.data['date'], "%Y-%m-%d") - DELTA_CORRECTION
                obj.data['date'] = weather_date.strftime('%Y-%m-%d')
                self.insert_into_side_table(obj, save_key='date')
            return
        try:
            join_data = self.join(message.payload)
            if join_data:
                hashes = self.hash_message(message=join_data, hashing_key='date', hash_modulo=self._consumers)
                for routing_key_postfix, message_buffer in hashes.items():
                    msg = Message(message_type=NULL_TYPE, payload=message_buffer)
                    raw_msg = Protocol.serialize_message(msg)
                    self._middleware.send_aggregator_message(raw_msg,
                                                             routing_key_postfix)
        except BaseException as e:
            logging.error(f'action: on-message-callback | result: failed | reason {e}')
            raise e from e

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        if message.is_type(WEATHER):
            self._middleware.cancel_consuming_weather()
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id))
            self._middleware.send_static_data_ack(ack, client_id)
        if message.is_type(TRIPS):
            eof = Message.build_eof_message(client_id=client_id)
            raw_eof = Protocol.serialize_message(eof)
            for i in range(self._consumers):
                self._middleware.send_aggregator_message(raw_eof, i)
            self._middleware.stop()

    def close(self):
        if not self.closed:
            super(JoinByDate, self).close()
            self._middleware.stop()
