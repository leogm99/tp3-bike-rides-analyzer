from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union, Set
from common_utils.protocol.payload import Payload
import logging

EOF = 'EOF'
STATIONS = 'stations'
WEATHER = 'weather'
TRIPS = 'trips'
METRICS = 'metrics'
DISTANCE_METRIC = 'distance_metric'
DURATION_METRIC = 'duration_metric'
COUNT_METRIC = 'count_metric'
ACK = 'ACK'
NOTIFY = 'notify'
NULL_TYPE = ''
TYPE_FIELD = 'type'
PAYLOAD_FIELD = 'payload'
ID = 'id'
MESSAGE_ID_FIELD = 'message_id'
CLIENT_ID = 'client_id'
ORIGIN = 'origin'
FLUSH = 'flush'
TIMESTAMP = 'timestamp'

@dataclass
class Message:
    message_type: str
    payload: Union[List[Payload], Payload]
    message_id: Union[int, None] = None
    origin: Union[str, None] = None
    client_id: Union[str, None] = None
    timestamp: Union[str, None] = None

    @staticmethod
    def build_eof_message(message_type='', client_id: str = '', origin: str = '', timestamp=None):
        if message_type == '':
            return Message(message_type=NULL_TYPE, payload=Payload(data={EOF: True}), origin=origin, client_id=client_id, timestamp=timestamp)
        return Message(message_type=message_type, payload=Payload(data={EOF: True}), origin=origin, client_id=client_id, timestamp=timestamp)

    @staticmethod
    def build_ack_message(client_id: str = '', origin: str = '', timestamp=None):
        return Message(message_type=NOTIFY, payload=Payload(data={ACK: True}), origin=origin, client_id=client_id, timestamp=timestamp)

    @staticmethod
    def build_id_message(id: str):
        return Message(message_type=ID, payload=Payload(data=id))
    
    @staticmethod
    def build_flush_message(timestamp):
        return Message(message_type=FLUSH, timestamp=timestamp, payload=Payload(data={}))

    def is_type(self, message_type):
        return self.message_type == message_type

    def is_eof(self):
        return isinstance(self.payload, Payload) and self.payload.is_eof()

    def is_ack(self):
        return isinstance(self.payload, Payload) and self.payload.is_ack()
    
    def is_id(self):
        return isinstance(self.payload, Payload) and self.message_type == ID

    def is_flush(self):
        return isinstance(self.payload, Payload) and self.message_type == FLUSH

    def pick_payload_fields(self, fields_set: Set):
        new_payload = []
        for obj in self.payload:
            new_payload.append(obj.pick_payload_fields(fields_set))
        return Message(message_type=self.message_type,
                       origin=self.origin,
                       client_id=self.client_id,
                       message_id=self.message_id,
                       timestamp=self.timestamp,
                       payload=new_payload)

    def into_dict(self):
        raw_message = defaultdict()
        if self.message_id is not None: 
            raw_message[MESSAGE_ID_FIELD] = self.message_id
        if self.origin is not None:
            raw_message[ORIGIN] = self.origin
        if self.client_id is not None:
            raw_message[CLIENT_ID] = self.client_id
        if self.timestamp is not None:
            raw_message[TIMESTAMP] = self.timestamp
        if self.message_type != NULL_TYPE:
            raw_message[TYPE_FIELD] = self.message_type
        if isinstance(self.payload, list):
            raw_message[PAYLOAD_FIELD] = list(map(lambda p: p.data, self.payload))
        elif isinstance(self.payload, Payload):
            raw_message[PAYLOAD_FIELD] = self.payload.data
        return raw_message

    @staticmethod
    def from_dict(raw_dict: defaultdict):
        msg = Message(message_type=NULL_TYPE, payload=[])
        msg.message_id = raw_dict.get(MESSAGE_ID_FIELD, None)
        msg.origin = raw_dict.get(ORIGIN, None)
        msg.message_type = raw_dict.get(TYPE_FIELD, None)
        msg.client_id = raw_dict.get(CLIENT_ID, None)
        msg.timestamp = raw_dict.get(TIMESTAMP, None)
        if isinstance(raw_dict[PAYLOAD_FIELD], list):
            payload = raw_dict[PAYLOAD_FIELD]
            for obj in payload:
                msg.payload.append(Payload(data=obj))
        else:
            msg.payload = Payload(data=raw_dict[PAYLOAD_FIELD])
        return msg
