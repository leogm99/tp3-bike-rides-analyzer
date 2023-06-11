from abc import ABC
from typing import Dict, Tuple
from common.aggregators.aggregator import Aggregator
from collections import Counter, defaultdict
from common_utils.KeyValueStore import KeyValueStore

CLIENT_ID = 'client_id'

class CountAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...]):
        super().__init__(aggregate_keys)
        self._aggregate_table: Dict[str, KeyValueStore] = {}
        self._aggregate_keys = aggregate_keys

    def aggregate(self, payload, **kwargs):
        client_id = self._get_client_id(payload)
        key = tuple(payload.data[i] for i in self._aggregate_keys)
        self._aggregate_table[client_id].update(key, **kwargs)

    def _get_client_id(self, payload):
        client_id = payload.data[CLIENT_ID]
        if client_id not in self._aggregate_table:
            self._aggregate_table[client_id] = KeyValueStore(defaultdict(Counter))
        return client_id
