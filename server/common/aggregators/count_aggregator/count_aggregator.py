from abc import ABC
from typing import Tuple
from common.aggregators.aggregator import Aggregator
from collections import Counter, defaultdict


class CountAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...]):
        super().__init__(aggregate_keys)
        self._aggregate_table = defaultdict(Counter)
        self._aggregate_keys = aggregate_keys

    def aggregate(self, payload, **kwargs):
        key = tuple(payload.data[i] for i in self._aggregate_keys)
        self._aggregate_table[key].update(**kwargs)

