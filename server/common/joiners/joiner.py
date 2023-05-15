from abc import ABC

from common.dag_node import DAGNode
from typing import Tuple, List, Union

from common_utils.protocol.payload import Payload


class Joiner(DAGNode, ABC):
    def __init__(self,
                 index_key: Tuple[str, ...]):
        super().__init__()
        self._index_key = tuple(sorted(index_key))
        self._side_table = {}

    def join(self, payload):
        if isinstance(payload, list):
            buffer = []
            for obj in payload:
                join_obj = self.__join(obj)
                if join_obj:
                    buffer.append(join_obj)
            return None if len(buffer) == 0 else buffer
        return self.__join(payload)

    def insert_into_side_table(self, payload: Union[Payload, List[Payload]], save_key: str = ''):
        if isinstance(payload, list):
            for obj in payload:
                self.__insert_into_side_table(obj, save_key)
            return
        self.__insert_into_side_table(payload, save_key)

    def __join(self, payload: Payload):
        key = tuple(payload.data[i] for i in self._index_key)
        if key in self._side_table:
            return Payload(data=self._side_table[key] | payload.data)
        return None

    def __insert_into_side_table(self, payload: Payload, save_key: str = ''):
        key = tuple(payload.data[i] for i in self._index_key)
        self._side_table[key] = {k: v for k, v in payload.data.items() if
                                 k not in self._index_key} if save_key == '' else \
            {save_key: payload.data[save_key]}
