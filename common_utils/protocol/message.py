from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union, Set
from common_utils.protocol.payload import Payload

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


@dataclass
class Message:
    message_type: str
    payload: Union[List[Payload], Payload]

    @staticmethod
    def build_eof_message(message_type=''):
        if message_type == '':
            return Message(message_type=NULL_TYPE, payload=Payload(data=EOF))
        return Message(message_type=message_type, payload=Payload(data=EOF))

    @staticmethod
    def build_ack_message():
        return Message(message_type=NOTIFY, payload=Payload(data=ACK))

    def is_type(self, message_type):
        return self.message_type == message_type

    def is_eof(self):
        return isinstance(self.payload, Payload) and self.payload.is_eof()

    def is_ack(self):
        return isinstance(self.payload, Payload) and self.payload.is_ack()

    def pick_payload_fields(self, fields_set: Set):
        new_payload = []
        for obj in self.payload:
            new_payload.append(obj.pick_payload_fields(fields_set))
        return Message(message_type=self.message_type, payload=new_payload)

    def into_dict(self):
        raw_message = defaultdict()
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
        if TYPE_FIELD in raw_dict:
            msg.message_type = raw_dict[TYPE_FIELD]
        if isinstance(raw_dict[PAYLOAD_FIELD], list):
            payload = raw_dict[PAYLOAD_FIELD]
            for obj in payload:
                msg.payload.append(Payload(data=obj))
        else:
            msg.payload = Payload(data=raw_dict[PAYLOAD_FIELD])
        return msg
