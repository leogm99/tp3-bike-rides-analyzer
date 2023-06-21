import dataclasses
import enum
import logging
import socket
import docker
import threading

from collections import defaultdict
from time import time
from typing import Any, List

from common.middleware.watcher_middleware import WatcherMiddleware
from queue import Queue

WATCHER_TIMEOUT_SEC = 30
WATCHER_RESTART_TIMEOUT = 25
WATCHER_RATE = 0.01

class HostState(enum.Enum):
    PROLLY_UP = enum.auto()
    RESTARTING = enum.auto()


@dataclasses.dataclass
class HostInfo:
    hostname: str
    heard_from_last: float
    last_restart_time: float = 0.
    curr_state: HostState = HostState.PROLLY_UP

    def reached_timeout(self, curr_time, timeout):
        return curr_time - self.heard_from_last > timeout

    def restarting(self, curr_time, timeout):
        return self.curr_state == HostState.RESTARTING and curr_time - self.last_restart_time < timeout

    def set_restart(self, restart_time):
        self.curr_state = HostState.RESTARTING
        self.last_restart_time = restart_time

    def update_info(self, heard_from):
        self.heard_from_last = heard_from
        self.curr_state = HostState.PROLLY_UP


class Watch:
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
                if found_host and found_host in self._monitored_hosts:
                    self._monitored_hosts[found_host].update_info(heard_from=curr_time)

            prolly_down_hosts = list(
                map(lambda h: h[1],
                    filter(lambda h:
                           h[1].reached_timeout(curr_time=curr_time, timeout=WATCHER_TIMEOUT_SEC) and not
                           h[1].restarting(curr_time=curr_time, timeout=WATCHER_RESTART_TIMEOUT),
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


class Reviver:
    def __init__(self) -> None:
        self._queue = Queue()

    def loop_revive(self):
        while True:
            data = self._queue.get()
            if not data:
                break
            for host in data:
                Reviver.__restart_container(host.hostname)
            
    def stop_reviving(self):
        self._queue.put(None)

    def schedule_revive(self, to_revive: List[HostInfo]):
        self._queue.put(to_revive)

    @staticmethod
    def __restart_container(container_ip_addr):
        client = docker.from_env()
        # TODO: explain/document this little hack with the container ip address string
        containers = list(filter(lambda c: container_ip_addr.split('.')[0] in c[1],
                                map(lambda c: (c, c.attrs['Name']), client.containers.list(all=True))))
        if containers:
            logging.info(f'Restarting: {containers[0][0]}')
            containers[0][0].restart()

