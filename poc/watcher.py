import dataclasses
import enum
import logging
import socket
import threading
from collections import defaultdict
from time import time
from typing import Any

from watcher_middleware import WatcherMiddleware

WATCHER_TIMEOUT_SEC = 10
WATCHER_RATE = 0.1

class HostState(enum.Enum):
    PROLLY_UP = enum.auto()
    RESTARTING = enum.auto()


@dataclasses.dataclass
class HostInfo:
    hostname: str
    heard_from_last: float
    curr_state: HostState = HostState.PROLLY_UP

    def reached_timeout(self, curr_time, timeout):
        return curr_time - self.heard_from_last > timeout

    def restarting(self):
        return self.curr_state == HostState.RESTARTING

    def set_restart(self):
        self.curr_state = HostState.RESTARTING

    def update_info(self, heard_from):
        self.heard_from_last = heard_from
        self.curr_state = HostState.PROLLY_UP


class Watcher:
    def __init__(self, hosts, on_failure_callback=lambda *_: Any):
        self._monitored_hosts = defaultdict()
        self._on_failure = on_failure_callback
        self._hosts = hosts
        self._comm = WatcherMiddleware(bind=True)
        self._stop_watch_event = threading.Event()

        curr_time = time()

        for host in hosts:
            self._monitored_hosts[host] = HostInfo(heard_from_last=curr_time, hostname=host)

    def watch(self):
        while not self._stop_watch_event.wait(timeout=WATCHER_RATE):
            curr_time = time()
            res = self._comm.receive_watcher_message()
            if res:
                sender_addr, _ = res
                found_host = self._resolve_host(sender_addr)
                if found_host:
                    self._monitored_hosts[found_host].update_info(heard_from=curr_time)

            prolly_down_hosts = list(
                map(lambda h: h[1],
                    filter(lambda h:
                           h[1].reached_timeout(curr_time=curr_time, timeout=WATCHER_TIMEOUT_SEC) and not
                           h[1].restarting(),
                           self._monitored_hosts.items()
                           )
                    )
            )
            if prolly_down_hosts:
                logging.info(f"Some hosts may have fallen: {prolly_down_hosts}")
                self._on_failure(prolly_down_hosts)
        self._comm.close()

    def _resolve_host(self, sender_addr):
        name = socket.getfqdn(sender_addr)
        found_host = None
        for host in self._hosts:
            if host in name:
                found_host = host
                break
        return found_host

    def stop_watch(self):
        self._stop_watch_event.set()
