import threading
import logging

from collections import defaultdict

from common_utils.singleton import Singleton
from common_utils.KeyValueStore import KeyValueStore


TIMESTAMP = 'timestamp.json'

'''
    Monotonic timestamps.

    Guarantees happened-before consistency between threads, independently of the operating system.
    It's obviously much less performant than a call to `time.monotonic` because
    each time a timestamps is stored, a mutex is taken and also the data needs to be flushed to disk.
'''
class TimestampMonitor(metaclass=Singleton):
    def __init__(self) -> None:
        self._kv_store = KeyValueStore.loads(TIMESTAMP, default_type=defaultdict(int))
        self._m = threading.Lock()
        logging.debug(f'last timestamp: {self._kv_store.get("timestamp")}')

    def get_new_timestamp(self):
        with self._m:
            self._kv_store['timestamp'] += 1
            self._kv_store.dumps('timestamp.json')
            return self._kv_store['timestamp']