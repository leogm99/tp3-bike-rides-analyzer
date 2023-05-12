from abc import ABC

from common.filters.filter import Filter
from typing import Any, Dict


class StringEquality(Filter, ABC):
    def __init__(self, filter_key: str,
                 filter_value: str,
                 keep_filter_key: bool = False):
        super().__init__(filter_key, keep_filter_key)
        self._filter_value = filter_value

    def filter_function(self, message_object: Dict[str, Any]):
        value = message_object[self._filter_key]
        return value == self._filter_value
