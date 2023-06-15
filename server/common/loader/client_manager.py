import threading


class Singleton(type):
    _instance = None
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class ClientManager(metaclass=Singleton):
    def __init__(self, max_clients=5):
        self._l = threading.Lock()
        self._condition = threading.Condition(self._l)
        self._counter = max_clients

    def add_client(self):
        with self._l:
            self._counter -= 1
            return self._counter == 0

    def remove_client(self):
        with self._l:
            self._counter += 1
            self._condition.notify_all()

    def wait_slot_available(self):
        with self._condition:
            self._condition.wait_for(lambda: self._counter > 0)
