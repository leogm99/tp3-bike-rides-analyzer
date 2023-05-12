from abc import ABC
from typing import Tuple, NamedTuple
from common.aggregators.aggregator import Aggregator


class AverageTuple(NamedTuple):
    current_count: int
    current_average: float


class RollingAverageAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...], average_key: str):
        super().__init__(aggregate_keys)
        self._aggregate_table = dict.fromkeys([], AverageTuple(0, 0))
        self._aggregate_keys = aggregate_keys
        self._average_key = average_key

    def aggregate(self, payload, **kwargs):
        key = tuple(payload[i] for i in self._aggregate_keys)
        average_value = self._aggregate_table.get(key, AverageTuple(0, 0))
        new_average = ((average_value.current_count * average_value.current_average) + float(
            payload[self._average_key])) / (average_value.current_count + 1)
        self._aggregate_table[key] = AverageTuple(average_value.current_count + 1, new_average)
