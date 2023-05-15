import abc

from common.dag_node import DAGNode
from typing import Any, Dict

from common_utils.protocol.message import Message


class Filter(DAGNode):
    def __init__(self, filter_key: str,
                 keep_filter_key: bool = False):
        super().__init__()
        self._filter_key = filter_key
        self._keep_filter_key = keep_filter_key

    @abc.abstractmethod
    def filter_function(self, message_object: Dict[str, Any]):
        pass

    def on_message_callback(self, message_obj: Message, delivery_tag):
        if not message_obj.is_eof():
            buffer = []
            if isinstance(message_obj.payload, list):
                for o in message_obj.payload:
                    to_send = self.__process_message_and_filter(o.data)
                    if to_send:
                        buffer.append(o)
                message_obj.payload = buffer
                return len(buffer) != 0, message_obj
            return self.__process_message_and_filter(message_obj.payload.data), message_obj
        return False, None

    def __process_message_and_filter(self, payload):
        to_send = self.filter_function(payload)
        if not to_send:
            return False
        if not self._keep_filter_key:
            del payload[self._filter_key]
        return True
