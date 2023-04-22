import abc
import signal
from typing import Any, Dict, List


class DAGNode(abc.ABC):
    def __init__(self):
        self.__register_sigterm_handle()
        self.closed = False

    @abc.abstractmethod
    def run(self):
        pass

    @staticmethod
    def select_dictionary_fields(obj: Dict[str, Any], fields: List[str]):
        return {k: obj[k] for k in fields}

    @abc.abstractmethod
    def close(self):
        pass

    def __register_sigterm_handle(self):
        signal.signal(signal.SIGTERM, lambda *_: self.close())
