from abc import ABC
from typing import Dict, Tuple, NamedTuple
from common.aggregators.aggregator import Aggregator
from functools import reduce
from collections import defaultdict
import logging

from common_utils.KeyValueStore import KeyValueStore

CLIENT_ID = 'client_id'
DATA = 'data'
MSG_IDS = 'msg_ids'

class AverageTuple(NamedTuple):
    current_count: int
    current_average: float


class RollingAverageAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...], average_key: str):
        super().__init__(aggregate_keys)
        self._aggregate_table: KeyValueStore = KeyValueStore.loads('aggregate_table.json',
                                                                   parsing_func=RollingAverageAggregator.parse_kv_store_raw_entries)
        self._aggregate_keys = aggregate_keys
        self._average_key = average_key

    def aggregate(self, payload, client_id, **kwargs):
        self._verify_client_id(client_id)
        if len(self._aggregate_keys) == 1:
            key = payload.data[self._aggregate_keys[0]]
        else:
            key = tuple(payload.data[i] for i in self._aggregate_keys)
        average_value: AverageTuple = self._aggregate_table[client_id][DATA][key]
        new_average = ((average_value.current_count * average_value.current_average) + float(
            payload.data[self._average_key])) / (average_value.current_count + 1)
        data = AverageTuple(average_value.current_count + 1, new_average)
        self._aggregate_table[client_id][DATA][key] = data

    def _verify_client_id(self, client_id):
        if client_id not in self._aggregate_table:
            self._aggregate_table[client_id] = defaultdict()
            self._aggregate_table[client_id][DATA] = defaultdict(lambda: AverageTuple(0, 0.))
            self._aggregate_table[client_id][MSG_IDS] = set()


    @staticmethod
    def parse_kv_store_raw_entries(raw_dict):
        try:
            res = defaultdict()
            for client_id, client_data in raw_dict.items():

                res[client_id] = defaultdict()
                res[client_id][DATA] = defaultdict(lambda: AverageTuple(0, 0.))
                res[client_id][MSG_IDS] = set()

                data = client_data[DATA]
                data_dict = defaultdict(lambda: AverageTuple(0,0.))
                for key, value in data.items():
                    current_count = int(value[0])
                    current_average = float(value[1])
                    data_dict[key] = AverageTuple(current_count=current_count, current_average=current_average)

                msg_ids = set(client_data[MSG_IDS])

                res[client_id][DATA] = data_dict
                res[client_id][MSG_IDS] = msg_ids
        except BaseException as e:
            logging.error(f'error en parser de rolling_average: {e}')

        return res