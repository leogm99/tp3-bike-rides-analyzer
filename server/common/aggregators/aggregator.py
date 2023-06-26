import abc
from typing import Tuple
from common.dag_node import DAGNode


# query 1: agregar por fecha, acumular promedio de duración de viaje (key: fecha, value: prom duracion)
# query 2: agregar por nombre de ciudad de inicio y año, sumar +1
# query 3: agregar por nombre de ciudad de fin, acumular promedio de distancia

MSG_IDS = 'msg_ids'


class Aggregator(DAGNode):
    def __init__(self, aggregate_keys: Tuple[str, ...]):
        super().__init__()
        self._aggregate_keys = aggregate_keys

    @abc.abstractmethod
    def aggregate(self, message, **kwargs):
        pass
        
    @staticmethod
    def was_message_processed(aggregate_table, message_id, client_id):
        if client_id in aggregate_table and message_id in aggregate_table[client_id][MSG_IDS]:
            return True
        return False

    @staticmethod
    def register_message_processed(aggregate_table, message_id, client_id):
        aggregate_table[client_id][MSG_IDS].add(message_id)