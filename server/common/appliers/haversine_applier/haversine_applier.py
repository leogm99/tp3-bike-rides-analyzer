import logging

from haversine import haversine

from common.appliers.applier import Applier
from common.appliers.haversine_applier.haversine_applier_middleware import HaversineApplierMiddleware
from common_utils.protocol.message import Message, NULL_TYPE, CLIENT_ID
from common_utils.protocol.protocol import Protocol

ORIGIN_PREFIX = 'haversine_applier'

class HaversineApplier(Applier):
    output_fields = {'end_station_name', 'distance'}

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

    def on_message_callback(self, message: Message, delivery_tag):
        new_payload = []
        for obj in message.payload:
            distance_calculated = self.apply(obj)
            obj.data['distance'] = distance_calculated
            obj.pick_payload_fields(self.output_fields)
            new_payload.append(obj)
        msg = Message(message_type=NULL_TYPE, 
                      origin=f"ORIGIN_PREFIX_{self._middleware._node_id}",
                      client_id=message.client_id,
                      message_id=message.message_id,
                      payload=new_payload)
        self.__send_message(msg)
        self._middleware.ack_message(delivery_tag)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        eof = Message.build_eof_message(message_type='', client_id=client_id, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
        raw_eof = Protocol.serialize_message(eof)
        for i in range(self._consumers):
            self._middleware.send_aggregator_message(
                raw_eof, i
            )
        

    def __send_message(self, message: Message):
        hashes = self.hash_message(message.payload, hashing_key='end_station_name', hash_modulo=self._consumers)
        for routing_key, buffer in hashes.items():
            if not buffer:
                continue
            msg = Message(message_type=NULL_TYPE, 
                          message_id=message.message_id,
                          client_id=message.client_id,
                          origin=message.origin,
                          payload=buffer)
            raw_msg = Protocol.serialize_message(msg)
            self._middleware.send_aggregator_message(raw_msg, routing_key)

    def apply(self, payload):
        start = float(payload.data[self._start_latitude_key]), float(payload.data[self._start_longitude_key])
        end = float(payload.data[self._end_latitude_key]), float(payload.data[self._end_longitude_key])
        return haversine(start, end)

    def close(self):
        if not self.closed:
            super(HaversineApplier, self).close()
            self._middleware.stop()
