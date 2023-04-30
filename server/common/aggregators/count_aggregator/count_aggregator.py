from abc import ABC
from typing import Tuple
from common.aggregators.aggregator import Aggregator
from collections import Counter, defaultdict


class CountAggregator(Aggregator, ABC):
    def __init__(self, rabbit_hostname: str, aggregate_keys: Tuple[str, ...]):
        super().__init__(rabbit_hostname, aggregate_keys)
        self._aggregate_table = defaultdict(Counter)
        self._aggregate_keys = aggregate_keys

    def aggregate(self, payload):
        key = tuple(payload[i] for i in self._aggregate_keys)
        self._aggregate_table[key].update(count=1)

