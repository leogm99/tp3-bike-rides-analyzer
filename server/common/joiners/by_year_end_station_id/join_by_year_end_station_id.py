import logging
from typing import Tuple

from common.joiners.by_year_end_station_id.join_by_year_end_station_id_middleware import \
    JoinByYearEndStationIdMiddleware
from common.joiners.joiner import Joiner

from common_utils.protocol.message import Message, TRIPS, STATIONS, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol
from common_utils.protocol.payload import Payload


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
            start_station_code = obj.data.pop('start_station_code')
            end_station_code = obj.data.pop('end_station_code')
            obj.data['code'] = start_station_code
            start_station_join = super(JoinByYearEndStationId, self).join(obj)
            obj.data['code'] = end_station_code
            end_station_join = super(JoinByYearEndStationId, self).join(obj)
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
            self.insert_into_side_table(message.payload)
            return
        buffer = self.join(message.payload)
        if buffer:
            msg = Message(message_type=NULL_TYPE, payload=buffer)
            self.__send_message(msg)

    def __send_message(self, message):
        raw_msg = Protocol.serialize_message(message)
        self._middleware.send_haversine_message(raw_msg)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        if message.is_type(STATIONS):
            self._middleware.cancel_consuming_stations()
            ack = Protocol.serialize_message(Message.build_ack_message(client_id=client_id))
            self._middleware.send_static_data_ack(ack)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        elif message.is_type(TRIPS):
            logging.info(f'action: on-producer-finished | received END OF STREAM: {message}')
            eof = Message.build_eof_message(client_id=client_id)
            for _ in range(self._consumers):
                self.__send_message(eof)
            self._middleware.stop()

    def close(self):
        if not self.closed:
            super(JoinByYearEndStationId, self).close()
            self._middleware.stop()
