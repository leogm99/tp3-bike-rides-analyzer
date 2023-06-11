import os
import logging
from typing import Callable
from time import sleep, time
from collections import defaultdict

import zmq
import threading
from enum import Flag, auto

PING_RATE_SECS = 1
WATCHER_RATE_SECS = 0.1


def zmq_retry_send(socket, payload, retry_count=3):
    current_retry = 0
    while current_retry != retry_count:
        try:
            socket.send_json(payload)
            return True
        except zmq.error.Again:
            pass
        except BaseException as e:
            logging.error(e)
            raise e
        current_retry += 1
        if current_retry == retry_count:
            logging.error(f'retry threshold')
    return False


def zmq_retry_recv(socket, retry_count=3):
    current_retry = 0
    while current_retry != retry_count:
        try:
            res = socket.recv_json()
            # logging.info(f"[{os.getenv('REPLICA_ID')}]: sent payload")
            return res
        except zmq.error.Again:
            pass
        except BaseException as e:
            logging.error(e)
            raise e
        current_retry += 1
        if current_retry == retry_count:
            logging.error('retry threshold')
    return None


class ElectionState(Flag):
    UNKNOWN_LEADER = auto()
    SEARCHING_LEADER = auto()
    LEADER_FOUND = auto()


# multithreaded singleton
class Singleton(type):
    _instance = None
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class ElectionStateMonitor(metaclass=Singleton):
    def __init__(self):
        self._l = threading.Lock()
        self._cv = threading.Condition(self._l)
        self._curr_state = ElectionState.UNKNOWN_LEADER
        self._leader_id = None

    def try_set_searching(self):
        """
            Sets SEARCHING_LEADER state if not already searching.
            Avoids race conditions over the state.

            this is a possible race condition:
            if (state == ElectionState.UNKNOWN_LEADER):
                set_state(ElectionState.SEARCHING_LEADER)

            Inbetween the if statement and the set_state call, another thread could possibly have set the state
            to searching previously, so we could have two or more threads doing the same work concurrently.

            To prevent this, the searching state is set atomically only if another thread is not already searching
            or if the leader state was set
        """
        with self._l:
            if not self._curr_state == ElectionState.SEARCHING_LEADER:
                self._curr_state = ElectionState.SEARCHING_LEADER
                return True
            return False

    def set_leader_found(self, leader_id):
        with self._l:
            self._curr_state = ElectionState.LEADER_FOUND
            self._leader_id = leader_id
            self._cv.notify_all()

    def set_unknown(self):
        with self._l:
            self._curr_state = ElectionState.UNKNOWN_LEADER
            self._leader_id = None

    def wait_leader_state(self):
        with self._cv:
            logging.info('wait_leader_state: waiting leader')
            self._cv.wait_for(lambda: self._curr_state == ElectionState.LEADER_FOUND)
            logging.info('wait_leader_state: leader found')
            return self._leader_id

    def is_leader_set(self):
        with self._l:
            return self._curr_state == ElectionState.LEADER_FOUND

    def im_leader(self):
        with self._l:
            return self._leader_id == int(os.getenv('REPLICA_ID')) if self._leader_id is not None else False


class Watcher:
    def __init__(self, replica_id) -> None:
        self._replica_id = replica_id


class Leader:
    def __init__(self) -> None:
        super().__init__()


class Follower:
    def __init__(self) -> None:
        super().__init__()


class LeaderElectionTrigger(threading.Thread):
    def __init__(self, replica_id, hosts_ids_mapping) -> None:
        super().__init__()
        self._hosts_ids_mapping = hosts_ids_mapping
        self._replica_id = replica_id
        self._done = False

    def run(self) -> None:
        replica_id = os.getenv('REPLICA_ID')
        ctx = zmq.Context.instance()

        if max(self._hosts_ids_mapping.keys()) == replica_id:
            self.broadcast_leader(ctx)
            return

        response = self.broadcast_election(ctx)

        # si no hay resultados, significa que soy el lider porque nadie me respondio
        if not response:
            self.broadcast_leader(ctx)

        self._done = True

    def broadcast_election(self, ctx):
        response = False
        for replica_id, h in self._hosts_ids_mapping.items():
            if replica_id <= self._replica_id:
                continue
            sock = ctx.socket(zmq.REQ)
            sock.RCVTIMEO = 1000
            sock.SNDTIMEO = 1000
            sock.connect(f'tcp://{h}:{bully_port}')
            res = zmq_retry_send(sock, {'type': 'election', 'id': self._replica_id})
            if not res:
                continue
            data = zmq_retry_recv(sock)
            if data:
                response = True
        return response

    def broadcast_leader(self, ctx):
        logging.info(f"[{os.getenv('REPLICA_ID')}] IM LEADER")
        for replica_id, h in self._hosts_ids_mapping.items():
            if replica_id == self._replica_id:
                continue
            sock = ctx.socket(zmq.REQ)
            sock.RCVTIMEO = 1000
            sock.SNDTIMEO = 1000
            sock.connect(f'tcp://{h}:{bully_port}')
            res = zmq_retry_send(sock, {'type': 'victory', 'id': self._replica_id})
            if res:
                _ = zmq_retry_recv(sock)

        ElectionStateMonitor().set_leader_found(self._replica_id)

    def is_done(self):
        return self._done


class LeaderElectionListener(threading.Thread):
    def __init__(self, replica_id, bully_port) -> None:
        super().__init__()
        self._replica_id = replica_id
        ctx = zmq.Context.instance()
        self._leader_election_channel = ctx.socket(zmq.REP)
        self._leader_election_channel.RCVTIMEO = 2000
        self._leader_election_channel.SNDTIMEO = 2000
        self._leader_election_channel.bind(f'tcp://*:{bully_port}')
        self._last_leader_election_trigger = None
        self._l = threading.Lock()

    def run(self):
        # TODO: unhardcode messages
        # self.try_start_new_leader_election()
        while True:
            try:
                res = self._leader_election_channel.recv_json()
            except zmq.error.Again:
                continue
            except BaseException as e:
                logging.info(f"action: leader_election_run | status: failed | reason: {e}")
                raise e

            if res['type'] == 'election':
                logging.info(f'[{self._replica_id}] Received election from {res["id"]}')
                _ = zmq_retry_send(self._leader_election_channel, {'type': 'answer'})
                self.try_start_new_leader_election()
            elif res['type'] == 'answer':
                _ = zmq_retry_send(self._leader_election_channel, {'type': 'PING'})
            elif res['type'] == 'victory':
                zmq_retry_send(self._leader_election_channel, {'type': 'PING'})
                ElectionStateMonitor().set_leader_found(res['id'])
                logging.info(f'[{os.getenv("REPLICA_ID")}] leader is: {res["id"]}')

    def try_start_new_leader_election(self):
        with self._l:
            election_state_monitor = ElectionStateMonitor()
            if election_state_monitor.try_set_searching():
                if self._last_leader_election_trigger:
                    self._last_leader_election_trigger.join()
                    self._last_leader_election_trigger = None
                new_leader_election = LeaderElectionTrigger(replica_id=self._replica_id,
                                                            hosts_ids_mapping={0: 'bully0', 1: 'bully1', 2: 'bully2', 3: 'bully3', 4: 'bully4'})
                new_leader_election.start()
                self._last_leader_election_trigger = new_leader_election


class Watchdog(threading.Thread):
    def __init__(self, hosts, on_failure=lambda *x: None):
        super().__init__()
        ctx = zmq.Context.instance()

        self._socket = ctx.socket(zmq.PULL)
        self._monitored_hosts = defaultdict()
        self._stop_event = threading.Event()
        self._on_failure = on_failure
        self._hosts = hosts

        curr_time = time()

        for host in hosts:
            self._socket.connect(f'tcp://{host}:{healthcheck_port}')
            self._monitored_hosts[host] = curr_time

        # TODO: config vars for timeout
        self._socket.RCVTIMEO = os.getenv('HEALTHCHECK_TIMEO', 1000)
        self._socket.SNDTIMEO = os.getenv('HEALTHCHECK_TIMEO', 1000)

    def run(self):
        # observo cada timeout segundos
        while not self._stop_event.wait(timeout=WATCHER_RATE_SECS):
            self.watch()

    def watch(self):
        recv = zmq_retry_recv(self._socket)
        current_time = time()
        if recv:
            logging.info(recv)
            if recv['hostname'] in self._monitored_hosts:
                self._monitored_hosts[recv['hostname']] = current_time
        for host, last_time in self._monitored_hosts.items():
            if current_time - last_time > watcher_timeout:
                logging.info(f'host {host} may have fallen')
                self._on_failure(host)


    def stop_watching(self):
        self._stop_event.set()


class Healthchecker:
    def __init__(self, hostname):
        super().__init__()
        ctx = zmq.Context.instance()
        self._socket = ctx.socket(zmq.PUSH)
        self._socket.RCVTIMEO = os.getenv('HEALTHCHECK_TIMEO', 1000)
        self._socket.SNDTIMEO = os.getenv('HEALTHCHECK_TIMEO', 1000)
        self._socket.bind(f'tcp://*:{healthcheck_port}')
        self._hostname = hostname

    def ping(self) -> None:
        zmq_retry_send(self._socket, {'type': 'HEALTH', 'hostname': self._hostname})


def bully(on_leader_callback: Callable, on_follower_callback: Callable):
    id_host_mapping = {0: 'bully0', 1: 'bully1', 2: 'bully2', 3: 'bully3', 4: 'bully4'}
    listener = LeaderElectionListener(replica_id=replica_id, bully_port=bully_port)
    listener.start()
    election_state_monitor = ElectionStateMonitor()
    while True:
        listener.try_start_new_leader_election()
        elected_leader = election_state_monitor.wait_leader_state()
        if elected_leader == replica_id:
            # we may return in the case a process with higher id joins the bully ring
            on_leader_callback(id_host_mapping)
        else:
            # if this function returns, the leader may have fallen
            on_follower_callback(elected_leader, id_host_mapping)
        election_state_monitor.set_unknown()


if __name__ == '__main__':
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    logging.info('RUNNING BULLY TEST')
    replica_id = int(os.getenv('REPLICA_ID'))
    bully_port = os.getenv('BULLY_PORT')
    healthcheck_port = os.getenv('HEALTHCHECK_PORT')
    watcher_timeout = float(os.getenv('WATCHER_TIMEOUT_SECS'))


    def leader_callback(id_host_mapping):
        leader_id = int(os.getenv('REPLICA_ID'))
        watch_dog = Watchdog(hosts=[v for k,v in id_host_mapping.items() if k != leader_id])
        watch_dog.start()
        healthchecker = Healthchecker(hostname=id_host_mapping[leader_id])
        election_state = ElectionStateMonitor()
        while election_state.im_leader():
            healthchecker.ping()
            sleep(PING_RATE_SECS)

        watch_dog.stop_watching()
        watch_dog.join()


    def follower_callback(leader, id_host_mapping):
        replica_id = int(os.getenv('REPLICA_ID'))
        healthchecker = Healthchecker(hostname=id_host_mapping[replica_id])
        election_state = ElectionStateMonitor()

        watch_dog = Watchdog(hosts=[id_host_mapping[leader]], on_failure=lambda *_: election_state.set_unknown())
        watch_dog.start()

        while election_state.is_leader_set():
            healthchecker.ping()
            sleep(PING_RATE_SECS)

        watch_dog.stop_watching()
        watch_dog.join()


    bully(on_leader_callback=leader_callback, on_follower_callback=follower_callback)
