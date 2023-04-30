import abc
import json

from common.dag_node import DAGNode
from typing import Any, Dict


class Filter(DAGNode):
    def __init__(self, filter_key: str,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False):
        super().__init__(rabbit_hostname)
        self._filter_key = filter_key
        self._keep_filter_key = keep_filter_key

    @abc.abstractmethod
    def filter_function(self, message_object: Dict[str, Any]):
        pass

    def on_message_callback(self, message_obj, delivery_tag):
        payload = message_obj['payload']
        if payload != 'EOF':
            if isinstance(payload, list):
                buffer = []
                for o in payload:
                    to_send = self.__process_message_and_filter(o)
                    if to_send:
                        buffer.append(o)
                message_obj['payload'] = buffer
                return len(buffer) != 0, message_obj
            to_send = self.__process_message_and_filter(payload)
            message_obj['payload'] = payload
            return to_send, message_obj
        return False, None

    def __process_message_and_filter(self, payload):
        to_send = self.filter_function(payload)
        if not to_send:
            return False
        if not self._keep_filter_key:
            del payload[self._filter_key]
        return True
