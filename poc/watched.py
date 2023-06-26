from time import sleep
from watcher_middleware import WatcherMiddleware

WATCHER_PORT = 12346

class Watched:
    def __init__(self, watcher_hosts):
        self._comm = WatcherMiddleware(bind=False)
        self._watcher_hosts = watcher_hosts

    def ping_watchers(self):
        while True:
            for watcher in self._watcher_hosts:
                self._comm.send_watcher_message(watcher)
            sleep(1)

