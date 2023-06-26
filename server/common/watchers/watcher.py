import logging
import threading
import signal

from random import random
from time import sleep, time

from typing import List

from common.middleware.ping_middleware import PingMiddleware
from common.watched.watched import Watched
from common.watchers.leader_election import LeaderElection
from common.watchers.watch import Watch, Reviver, HostInfo

HOSTS_FILE = 'hosts.txt'


class Watcher(Watched):
    def __init__(self, watcher_id):
        super().__init__(ignore_watcher_id=watcher_id)
        self._watcher_id_host_mapping = self.load_watcher_info()
        self._leader_election = LeaderElection(watcher_id, self._watcher_id_host_mapping)
        self._leader_election_thread = threading.Thread(target=self._leader_election.listen_messages)
        self._leader_election_thread.start()
        self._ping = PingMiddleware()
        self._watcher = None
        self._watcher_thread = None
        self._reviver = None
        self._reviver_thread = None
        self._watcher_id = watcher_id
        self._closed = False
        self.__register_signal()

    def __register_signal(self):
        signal.signal(signalnum=signal.SIGTERM, handler=lambda *_: self.stop())

    def run(self):
        self.__bully()

    def stop(self):
        try:
            logging.info('action: watcher-stop | status: in progress')
            self._leader_election.stop()
            logging.debug('action: watcher-stop | message: stopped leader election')
            self._leader_election_thread.join()
            logging.debug('action: watcher-stop | message: joined thread')
            self._ping.close()
            logging.debug('action: watcher-stop | message: stopped ping')
            self._closed = True
            logging.info('action: watcher-stop | status: done')
            super().stop()
        except BaseException as e:
            raise e from e

    def __bully(self):
        hosts = self.read_hosts()
        this_watcher_host = self._watcher_id_host_mapping[self._watcher_id]
        hosts.remove(this_watcher_host)
        while not self._closed:
            if self._leader_election.am_i_leader():
                self.__leader(hosts)
            else:
                self.__follower()
        self.__try_release_leader_resources()

    def __follower(self):
        self.__try_release_leader_resources()

        logging.info('do follower shit')
        leader = self._leader_election.get_leader_id()
        logging.info(f'leader is {leader}')
        if leader is None:
            self._leader_election.find_leader(id_host_mapping=self._watcher_id_host_mapping)
            sleep(2 + random())
        else:
            if not self._ping.send_ping(self._watcher_id,
                                        self._watcher_id_host_mapping[leader]) or not self._ping.receive_ping():
                logging.info('maybe leader failed...')
                self._leader_election.find_leader(id_host_mapping=self._watcher_id_host_mapping)
        sleep(1 + random())

    def __try_release_leader_resources(self):
        if self._watcher:
            self._watcher.stop_watch()
            self._watcher_thread.join()
            self._watcher = None
            self._watcher_thread = None
        if self._reviver:
            self._reviver.stop_reviving()
            self._reviver_thread.join()
            self._reviver = None
            self._reviver_thread = None

    def __leader(self, hosts):
        self.__try_acquire_leader_resources(hosts)
        logging.info('do leader shit')
        ping_res = self._ping.receive_ping()
        if ping_res:
            self._ping.send_ping(self._watcher_id, self._watcher_id_host_mapping[ping_res[1]])
        sleep(0.5)

    def __try_acquire_leader_resources(self, hosts):
        if not self._watcher:
            self._watcher = Watch(hosts=hosts, on_failure_callback=self.__on_node_failure)
            self._watcher_thread = threading.Thread(target=self._watcher.watch)
            self._watcher_thread.start()
        if not self._reviver:
            self._reviver = Reviver()
            self._reviver_thread = threading.Thread(target=self._reviver.loop_revive)
            self._reviver_thread.start()

    def __on_node_failure(self, failed_hosts: List[HostInfo]):
        curr_time = time()
        for host in failed_hosts:
            host.set_restart(curr_time)
        if self._reviver:
            self._reviver.schedule_revive(failed_hosts)