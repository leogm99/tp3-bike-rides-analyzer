import abc
from abc import ABC

from common.dag_node import DAGNode


class Applier(DAGNode, ABC):
    def __init__(self, rabbit_hostname: str):
        super().__init__(rabbit_hostname)

    @abc.abstractmethod
    def apply(self, message):
        pass
