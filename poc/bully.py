import copy
import os
import logging
from time import sleep
from random import random

from leader_election import LeaderElection
from ping_middleware import PingMiddleware
from watcher import Watcher, HostInfo
from watched import Watched
from restart import restart_container

import threading

def on_failure(hosts: list[HostInfo]):
    # this may happen in a separate thread to avoid blocking!
    for host_info in hosts:
        host_info.set_restart()
        restart_container(host_info.hostname)

def bully():
    id_host_mapping = {0: 'bully0', 1: 'bully1', 2: 'bully2', 3: 'bully3', 4: 'bully4'}
    leader_election = LeaderElection(replica_id, id_host_mapping)
    t = threading.Thread(target=leader_election.listen_messages)
    t.start()
    ping = PingMiddleware()
    hosts = [v for k,v in id_host_mapping.items() if k != replica_id]
    watched = Watched(watcher_hosts=hosts)
    watched_thread = threading.Thread(target=watched.ping_watchers)
    watched_thread.start()
    watcher = None
    watcher_thread = None


    while True:
        if leader_election.am_i_leader():
            if not watcher:
                watcher = Watcher(hosts=copy.deepcopy(hosts), on_failure_callback=on_failure)
                watcher_thread = threading.Thread(target=watcher.watch)
                watcher_thread.start()
            logging.info('do leader shit')
            ping_res = ping.receive_ping()
            if ping_res:
                ping.send_ping(replica_id, id_host_mapping[ping_res[1]])
            sleep(0.1)
        else:
            if watcher:
                watcher.stop_watch()
                watcher_thread.join()
                watcher = None
                watcher_thread = None

            logging.info('do follower shit')
            leader = leader_election.get_leader_id()
            logging.info(f'leader is {leader}')
            if leader is None:
                sleep(1 + random())
                leader_election.find_leader(id_host_mapping=id_host_mapping)
            else:
                if not ping.send_ping(replica_id, id_host_mapping[leader]) or not ping.receive_ping():
                    logging.info('maybe leader failed...')
                    leader_election.find_leader(id_host_mapping=id_host_mapping)
            sleep(1 + random())


if __name__ == '__main__':
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    logging.info('RUNNING BULLY TEST')
    replica_id = int(os.getenv('REPLICA_ID'))
    bully_port = int(os.getenv('BULLY_PORT'))
    healthcheck_port = int(os.getenv('HEALTHCHECK_PORT'))
    watcher_timeout = float(os.getenv('WATCHER_TIMEOUT_SECS'))

    bully()
