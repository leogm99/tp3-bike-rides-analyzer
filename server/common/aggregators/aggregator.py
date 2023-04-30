import abc
from typing import Tuple
from common.dag_node import DAGNode


# query 1: agregar por fecha, acumular promedio de duración de viaje (key: fecha, value: prom duracion)
# query 2: agregar por nombre de ciudad de inicio y año, sumar +1
# query 3: agregar por nombre de ciudad de fin, acumular promedio de distancia


class Aggregator(DAGNode):
    def __init__(self, 
                 rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...]):
        super().__init__(rabbit_hostname)
        self._aggregate_keys = aggregate_keys

    @abc.abstractmethod
    def aggregate(self, message):
        pass
    
