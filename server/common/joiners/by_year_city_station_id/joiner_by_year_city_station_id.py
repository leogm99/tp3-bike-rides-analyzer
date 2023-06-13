import json
import logging
from typing import Tuple

from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id_middleware import \
    JoinByYearCityStationIdMiddleware
from common.joiners.joiner import Joiner
from common_utils.protocol.message import Message, TRIPS, STATIONS, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol


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
        buffer = []
        for obj in payload:
            obj.data['code'] = obj.data.pop('start_station_code')
            join_data = super(JoinByYearCityStationId, self).join(obj)
            if join_data is not None:
                del join_data.data['city']
                del join_data.data['code']
                buffer.append(join_data)
        return buffer

    def on_message_callback(self, message_obj: Message, delivery_tag):
        if message_obj.is_eof():
            return
        if message_obj.is_type(STATIONS):
            self.insert_into_side_table(message_obj.payload)
            return
        join_data = self.join(message_obj.payload)
        if join_data:
            hashes = self.hash_message(message=join_data, hashing_key='name', hash_modulo=self._consumers)
            for routing_key_suffix, obj in hashes.items():
                msg = Message(message_type=NULL_TYPE, payload=obj)
                raw_msg = Protocol.serialize_message(msg)
                self._middleware.send_aggregate_message(raw_msg, routing_key_suffix)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        if message.is_type(STATIONS):
            self._middleware.cancel_consuming_stations()
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id))
            self._middleware.send_static_data_ack(ack)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        if message.is_type(TRIPS):
            eof = Message.build_eof_message(client_id=client_id)
            raw_eof = Protocol.serialize_message(eof)
            for i in range(self._consumers):
                self._middleware.send_aggregate_message(raw_eof, i)
            self._middleware.stop()

    def close(self):
        if not self.closed:
            super(JoinByYearCityStationId, self).close()
            self._middleware.stop()
