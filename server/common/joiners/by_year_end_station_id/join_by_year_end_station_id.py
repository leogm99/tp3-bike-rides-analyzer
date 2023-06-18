import logging
from typing import Tuple

from common.joiners.by_year_end_station_id.join_by_year_end_station_id_middleware import \
    JoinByYearEndStationIdMiddleware
from common.joiners.joiner import Joiner

from common_utils.protocol.message import Message, TRIPS, STATIONS, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol
from common_utils.protocol.payload import Payload

ORIGIN_PREFIX = 'joiner_by_year_end_station_id'

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

    def join(self, payload, client_id):
        buffer = []
        for obj in payload:
            start_station_code = obj.data.pop('start_station_code')
            end_station_code = obj.data.pop('end_station_code')
            obj.data['code'] = start_station_code
            start_station_join = super(JoinByYearEndStationId, self).join(obj, client_id)
            obj.data['code'] = end_station_code
            end_station_join = super(JoinByYearEndStationId, self).join(obj, client_id)
            if start_station_join and end_station_join:
                start_station_join.data['start_station_latitude'] = start_station_join.data.pop('latitude')
                start_station_join.data['start_station_longitude'] = start_station_join.data.pop('longitude')
                end_station_join.data['end_station_latitude'] = end_station_join.data.pop('latitude')
                end_station_join.data['end_station_longitude'] = end_station_join.data.pop('longitude')
                end_station_join.data['end_station_name'] = end_station_join.data.pop('name')
                del start_station_join.data['name']
                del start_station_join.data['code']
                del start_station_join.data['yearid']
                del end_station_join.data['yearid']
                del end_station_join.data['code']
                result = Payload(data=start_station_join.data | end_station_join.data)
                buffer.append(result)
        return buffer

    def on_message_callback(self, message, delivery_tag):
        if message.is_eof():
            return
        if message.is_type(STATIONS):
            self.insert_into_side_table(message.payload, client_id=message.client_id)
            return
        buffer = self.join(message.payload, client_id=message.client_id)
        if buffer:
            #logging.info('could join')
            routing_key = int(message.message_id) % self._consumers
            msg = Message(message_type=NULL_TYPE,
                          message_id=message.message_id,
                          client_id=message.client_id,
                          origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}",
                          payload=buffer)
            self.__send_message(msg, routing_key)

    def __send_message(self, message, routing_key):
        raw_msg = Protocol.serialize_message(message)
        self._middleware.send_haversine_message(raw_msg, routing_key)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        if message.is_type(STATIONS):
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id))
            self._middleware.send_static_data_ack(ack, client_id)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        elif message.is_type(TRIPS):
            logging.info(f'action: on-producer-finished | received END OF STREAM: {message}')
            eof = Message.build_eof_message(message_type='', client_id=client_id, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
            for i in range(self._consumers):
                self.__send_message(eof, i)
            

    def close(self):
        if not self.closed:
            super(JoinByYearEndStationId, self).close()
            self._middleware.stop()
