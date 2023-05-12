import abc
from abc import ABC

from common.dag_node import DAGNode


class Applier(DAGNode, ABC):
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def apply(self, message):
        pass
