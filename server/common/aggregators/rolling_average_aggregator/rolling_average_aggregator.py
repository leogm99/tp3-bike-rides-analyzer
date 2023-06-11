from abc import ABC
from typing import Dict, Tuple, NamedTuple
from common.aggregators.aggregator import Aggregator

from common_utils.KeyValueStore import KeyValueStore

CLIENT_ID = 'client_id'

class AverageTuple(NamedTuple):
    current_count: int
    current_average: float


class RollingAverageAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...], average_key: str):
        super().__init__(aggregate_keys)
        self._aggregate_table: Dict[str, KeyValueStore] = {}
        self._aggregate_keys = aggregate_keys
        self._average_key = average_key

    def aggregate(self, payload, **kwargs):
        client_id = self._get_client_id(payload)
        key = tuple(payload.data[i] for i in self._aggregate_keys)
        average_value: AverageTuple = self._aggregate_table[client_id].get(key, AverageTuple(0, 0))
        new_average = ((average_value.current_count * average_value.current_average) + float(
            payload.data[self._average_key])) / (average_value.current_count + 1)
        data = AverageTuple(average_value.current_count + 1, new_average)
        self._aggregate_table[client_id].put(key, data)

    def _get_client_id(self, payload):
        client_id = payload.data[CLIENT_ID]
        if client_id not in self._aggregate_table:
            self._aggregate_table[client_id] = KeyValueStore(dict.fromkeys([], AverageTuple(0, 0)))
        return client_id
