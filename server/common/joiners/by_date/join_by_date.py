import json
import logging
from collections import defaultdict
from typing import Tuple
from datetime import datetime, timedelta

from common.joiners.by_date.join_by_date_middleware import JoinByDateMiddleware
from common.joiners.joiner import Joiner
from common_utils.protocol.message import Message, TRIPS, WEATHER, NULL_TYPE, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore

DELTA_CORRECTION = timedelta(days=1)
ORIGIN_PREFIX = 'join_by_date'

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
            self._side_table = KeyValueStore.loads(f"{ORIGIN_PREFIX}_{self._middleware._node_id}", default_type=defaultdict(dict))
            self._middleware.consume_flush(f"{FLUSH}_{ORIGIN_PREFIX}_{self._middleware._node_id}", self.on_flush)
            self._middleware.receive_weather(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def join(self, payload, client_id):
        join_data = []
        for obj in payload:
            obj.data['date'] = obj.data.pop('start_date')
            trip_date = datetime.strptime(obj.data['date'], '%Y-%m-%d %H:%M:%S')
            obj.data['date'] = datetime.strftime(trip_date, '%Y-%m-%d')
            join_payload = super(JoinByDate, self).join(obj, client_id)
            if join_payload is not None:
                del join_payload.data['city']
                join_data.append(join_payload)
        return join_data

    def on_message_callback(self, message: Message, delivery_tag):
        if message.is_type(WEATHER):
            for obj in message.payload:
                weather_date = datetime.strptime(obj.data['date'], "%Y-%m-%d") - DELTA_CORRECTION
                obj.data['date'] = weather_date.strftime('%Y-%m-%d')
                self.insert_into_side_table(obj, save_key='date', client_id=message.client_id)
            if self._middleware.save_weather_delivery_tag(delivery_tag):
                self._side_table.dumps(f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
                self._middleware.ack_weathers()
            return
        try:
            join_data = self.join(message.payload, message.client_id)
            if join_data:
                hashes = self.hash_message(message=join_data, hashing_key='date', hash_modulo=self._consumers)
                for routing_key_postfix, message_buffer in hashes.items():
                    if not message_buffer:
                        continue
                    msg = Message(message_type=NULL_TYPE,
                                  client_id=message.client_id,
                                  message_id=message.message_id,
                                  timestamp=message.timestamp,
                                  origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}",
                                  payload=message_buffer)
                    raw_msg = Protocol.serialize_message(msg)
                    self._middleware.send_aggregator_message(raw_msg,
                                                             routing_key_postfix)
            self._middleware.ack_trip(delivery_tag)
        except BaseException as e:
            logging.error(f'action: on-message-callback | result: failed | reason {e}')
            raise e from e

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        timestamp = message.timestamp
        if message.is_type(WEATHER):
            self._side_table.dumps(f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}"))
            self._middleware.send_static_data_ack(ack, client_id)
            self._middleware.ack_weathers()
        if message.is_type(TRIPS):
            eof = Message.build_eof_message(message_type='', client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            raw_eof = Protocol.serialize_message(eof)
            for i in range(self._consumers):
                self._middleware.send_aggregator_message(raw_eof, i)
            
            if client_id in self._side_table:
                self._side_table.delete(client_id)

    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)
        self._side_table.nuke(f"{ORIGIN_PREFIX}_{self._middleware._node_id}")

    def close(self):
        if not self.closed:
            super(JoinByDate, self).close()
            self._middleware.stop()
