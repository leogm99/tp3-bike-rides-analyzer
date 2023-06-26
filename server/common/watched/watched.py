import threading
from common.middleware.watcher_middleware import WatcherMiddleware

WATCHED_RATE = 1
HOSTS_FILE = 'hosts.txt'

class Watched:
    def __init__(self, ignore_watcher_id=None):
        self._comm = WatcherMiddleware(bind=False)
        watcher_ids_hosts = Watched.load_watcher_info()
        self._stop_event = threading.Event()
        self._t = None
        self._watcher_hosts = [v for k,v in watcher_ids_hosts.items() if k != ignore_watcher_id]

        self.__ping_watchers()

    def __ping_watchers(self):
        if not self._t:
            self._t = threading.Thread(target=self.__ping)
            self._t.start()
        else:
            raise ValueError("Cannot restart watched thread")

    def stop(self):
        self._stop_event.set()
        if self._t:
            self._t.join()
            self._t = None

    def __ping(self):
        while not self._stop_event.wait(timeout=WATCHED_RATE):
            for watcher in self._watcher_hosts:
                self._comm.send_watcher_message(watcher)
        self._comm.close()

    @staticmethod
    def read_hosts():
        with open(HOSTS_FILE, 'r') as h:
            return h.read().splitlines()

    @staticmethod
    def load_watcher_info():
        import re
        pattern = re.compile(r'watcher_(\d+)')
        hosts = Watched.read_hosts()
        watcher_hosts = list(filter(lambda l: 'watcher' in l, hosts))
        id_hosts = {}
        for host in watcher_hosts:
            id_hosts[int(pattern.search(host).group(1))] = host
        return id_hosts

