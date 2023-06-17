from abc import ABC
from typing import Dict

from common.dag_node import DAGNode
from typing import Tuple, List, Union

from common_utils.protocol.payload import Payload
from common_utils.KeyValueStore import KeyValueStore

CLIENT_ID = 'client_id'

class Joiner(DAGNode, ABC):
    def __init__(self,
                 index_key: Tuple[str, ...]):
        super().__init__()
        self._index_key = tuple(sorted(index_key))
        self._side_table: Dict[str, KeyValueStore] = {}

    def join(self, payload, client_id: str = None):
        if isinstance(payload, list):
            buffer = []
            for obj in payload:
                join_obj = self.__join(obj, client_id)
                if join_obj:
                    buffer.append(join_obj)
            return None if len(buffer) == 0 else buffer
        return self.__join(payload, client_id)

    def insert_into_side_table(self, payload: Union[Payload, List[Payload]], save_key: str = '', client_id: str = None):
        if isinstance(payload, list):
            for obj in payload:
                self._verify_client_id(client_id)
                self.__insert_into_side_table(obj, save_key, client_id)
            return
        self._verify_client_id(client_id)
        self.__insert_into_side_table(payload, save_key, client_id)

    def __join(self, payload: Payload, client_id: str = None):
        key = tuple(payload.data[i] for i in self._index_key)
        data = self._side_table[client_id].get(key)
        if data != None:
            return Payload(data=data | payload.data)
        return None

    def __insert_into_side_table(self, payload: Payload, save_key: str = '', client_id: str = None):
        key = tuple(payload.data[i] for i in self._index_key)
        data = {k: v for k, v in payload.data.items() if
                                 k not in self._index_key} if save_key == '' else \
            {save_key: payload.data[save_key]}
        self._side_table[client_id].put(key, data)
    
    def _verify_client_id(self, client_id):
        if client_id not in self._side_table:
            self._side_table[client_id] = KeyValueStore()
