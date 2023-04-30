from abc import ABC

from common.filters.filter import Filter
from typing import Any, Dict


class NumericRange(Filter, ABC):
    def __init__(self, filter_key: str,
                 low: float,
                 high: float,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False):
        super().__init__(filter_key, rabbit_hostname, keep_filter_key)
        self._low = low
        self._high = high

    def filter_function(self, message_object: Dict[str, Any]):
        value = float(message_object[self._filter_key])
        return self._low <= value <= self._high
