from abc import ABC
from typing import Dict, Tuple
from common.aggregators.aggregator import Aggregator
from collections import Counter, defaultdict
from common_utils.KeyValueStore import KeyValueStore

CLIENT_ID = 'client_id'
DATA = 'data'
MSG_IDS = 'msg_ids'

class CountAggregator(Aggregator, ABC):
    def __init__(self, aggregate_keys: Tuple[str, ...]):
        super().__init__(aggregate_keys)
        self._aggregate_table: KeyValueStore = KeyValueStore.loads('aggregate_table.json', 
                                                                   parsing_func=CountAggregator.parse_kv_store_raw_entries)
        self._aggregate_keys = aggregate_keys

    def aggregate(self, payload, client_id, **kwargs):
        self._verify_client_id(client_id)
        if len(self._aggregate_keys) == 1:
            key = payload.data[self._aggregate_keys[0]]
        else:
            key = tuple(payload.data[i] for i in self._aggregate_keys)
        self._aggregate_table[client_id][DATA][key].update(**kwargs)

    def _verify_client_id(self, client_id):
        if client_id not in self._aggregate_table:
            self._aggregate_table[client_id] = defaultdict()
            self._aggregate_table[client_id][DATA] = defaultdict(Counter)
            self._aggregate_table[client_id][MSG_IDS] = set()

    @staticmethod
    def parse_kv_store_raw_entries(raw_dict: Dict):
        res = defaultdict()
        for client_id, client_data in raw_dict.items():

            res[client_id] = defaultdict()
            res[client_id][DATA] = defaultdict(Counter)
            res[client_id][MSG_IDS] = set()

            data = client_data[DATA]
            data_dict = defaultdict(Counter)
            for key, value in data.items():
                data_dict[key].update(value)

            msg_ids = set(client_data[MSG_IDS])

            res[client_id][DATA] = data_dict
            res[client_id][MSG_IDS] = msg_ids

        return res
    
