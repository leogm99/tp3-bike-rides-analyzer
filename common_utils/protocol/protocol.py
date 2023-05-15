import json
import struct

from common_utils.protocol.message import Message

ENCODING = 'utf8'
STRING_LENGTH = 4


class Protocol:
    @staticmethod
    def serialize_message(message: Message):
        raw_json_dict = message.into_dict()
        return Protocol.encode_message(json.dumps(raw_json_dict))

    @staticmethod
    def deserialize_message(raw_bytes: bytes):
        raw_json_str = Protocol.decode_message(raw_bytes)
        json_dict = json.loads(raw_json_str)
        msg = Message.from_dict(json_dict)
        return msg

    @staticmethod
    def encode_message(raw_str: str):
        return raw_str.encode(ENCODING)

    @staticmethod
    def decode_message(raw_bytes: bytes):
        return raw_bytes.decode(ENCODING)

    @staticmethod
    def receive_message(recv_handle):
        payload_size_buffer = recv_handle(STRING_LENGTH)
        payload_size = struct.unpack(f'!I', payload_size_buffer)[0]
        string_payload = recv_handle(payload_size)
        raw_bytes = struct.unpack(f'{payload_size}s', string_payload)[0]
        return Protocol.deserialize_message(raw_bytes)

    @staticmethod
    def send_message(send_handle, message):
        raw_bytes = Protocol.serialize_message(message)
        len_encoding = len(raw_bytes)
        payload = struct.pack(f'!I{len_encoding}s', len_encoding, raw_bytes)
        send_handle(payload)
