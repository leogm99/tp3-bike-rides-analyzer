import json
import logging
from typing import Tuple

from collections import defaultdict
from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id_middleware import \
    JoinByYearCityStationIdMiddleware
from common.joiners.joiner import Joiner
from common_utils.protocol.message import Message, TRIPS, STATIONS, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore

ORIGIN_PREFIX = 'joiner_by_year_city_station_id'

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
            self._side_table = KeyValueStore.loads(f"{ORIGIN_PREFIX}_{self._middleware._node_id}", default_type=defaultdict(dict))
            self._middleware.receive_stations(self.on_message_callback, self.on_producer_finished)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def join(self, payload, client_id):
        buffer = []
        for obj in payload:
            obj.data['code'] = obj.data.pop('start_station_code')
            join_data = super(JoinByYearCityStationId, self).join(obj, client_id)
            if join_data is not None:
                del join_data.data['city']
                del join_data.data['code']
                buffer.append(join_data)
        return buffer

    def on_message_callback(self, message_obj: Message, delivery_tag):
        if message_obj.is_type(STATIONS):
            self.insert_into_side_table(message_obj.payload, client_id=message_obj.client_id)
            if self._middleware.save_stations_delivery_tag(delivery_tag):
                self._side_table.dumps(f'{ORIGIN_PREFIX}_{self._middleware._node_id}')
                self._middleware.ack_stations()

            return
        join_data = self.join(message_obj.payload, client_id=message_obj.client_id)
        if join_data:
            hashes = self.hash_message(message=join_data, hashing_key='name', hash_modulo=self._consumers)
            for routing_key_suffix, obj in hashes.items():
                if not obj:
                    continue
                msg = Message(message_type=NULL_TYPE,
                              message_id=message_obj.message_id,
                              client_id=message_obj.client_id,
                              origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}",
                              payload=obj)
                raw_msg = Protocol.serialize_message(msg)
                self._middleware.send_aggregate_message(raw_msg, routing_key_suffix)
        self._middleware.ack_trip(delivery_tag)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        if message.is_type(STATIONS):
            self._side_table.dumps(f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}"))
            self._middleware.send_static_data_ack(ack, client_id)
            self._middleware.ack_stations()
        if message.is_type(TRIPS):
            eof = Message.build_eof_message(message_type='', client_id=client_id, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            raw_eof = Protocol.serialize_message(eof)
            for i in range(self._consumers):
                self._middleware.send_aggregate_message(raw_eof, i)
            

    def close(self):
        if not self.closed:
            super(JoinByYearCityStationId, self).close()
            self._middleware.stop()
