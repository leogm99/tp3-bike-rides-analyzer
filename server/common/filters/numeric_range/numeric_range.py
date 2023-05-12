from abc import ABC

from common.filters.filter import Filter
from typing import Any, Dict


class NumericRange(Filter, ABC):
    def __init__(self, filter_key: str,
                 low: float,
                 high: float,
                 keep_filter_key: bool = False):
        super().__init__(filter_key, keep_filter_key)
        self._low = low
        self._high = high

    def filter_function(self, message_object: Dict[str, Any]):
        value = float(message_object[self._filter_key])
        return self._low <= value <= self._high

    @property
    def low(self):
        return self._low

    @low.setter
    def low(self, new_low):
        self._low = new_low

    @property
    def high(self):
        return self._high

    @high.setter
    def high(self, new_high):
        self._high = new_high

