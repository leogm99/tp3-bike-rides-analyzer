import os
import logging
import zmq
import threading
from enum import Flag, auto


def zmq_retry_send(socket, payload, retry_count=3):
    current_retry = 0
    while current_retry != retry_count:
        try:
            socket.send_json(payload)
            logging.info(f"[{os.getenv('REPLICA_ID')}]: sent payload")
            return True
        except zmq.error.Again:
            pass
        except BaseException as e:
            raise e
        current_retry += 1
        if current_retry == retry_count:
            logging.error('replica is down')
    return False


def zmq_retry_recv(socket, retry_count=3):
    current_retry = 0
    while current_retry != retry_count:
        try:
            res = socket.recv_json()
            logging.info(f"[{os.getenv('REPLICA_ID')}]: sent payload")
            return res
        except zmq.error.Again:
            pass
        except BaseException as e:
            raise e
        current_retry += 1
        if current_retry == retry_count:
            logging.error('replica is down')
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
            Sets SEARCHING_LEADER state if not already searching or leader found.
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
            if not self._curr_state == ElectionState.SEARCHING_LEADER and not self._curr_state == ElectionState.LEADER_FOUND:
                self._curr_state = ElectionState.SEARCHING_LEADER
                return True
            return False

    def set_leader_found(self, leader_id):
        with self._l:
            self._curr_state = ElectionState.LEADER_FOUND
            self._cv.notify_all()
            self._leader_id = leader_id

    def set_unknown(self):
        with self._l:
            self._curr_state = ElectionState.UNKNOWN_LEADER
            self._leader_id = None

    def wait_leader_state(self):
        with self._cv:
            self._cv.wait_for(lambda: self._curr_state == ElectionState.LEADER_FOUND)
            return self._leader_id


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
        logging.info("running leader election")
        ctx = zmq.Context.instance()
        results = []
        election_state = ElectionStateMonitor()
        if not election_state.try_set_searching():
            self._done = True
            return
        for replica_id, h in self._hosts_ids_mapping.items():
            if replica_id <= self._replica_id:
                continue
            sock = ctx.socket(zmq.REQ)
            sock.RCVTIMEO = 1000
            sock.SNDTIMEO = 1000
            sock.connect(f'tcp://{h}:12345')
            res = zmq_retry_send(sock, {'type': 'election', 'id': self._replica_id})
            if not res:
                continue
            data = zmq_retry_recv(sock)
            if data:
                results.append(data)

        # si no hay resultados, significa que soy el lider porque nadie me respondio
        if not results:
            logging.info(f"[{os.getenv('REPLICA_ID')}] IM LEADER")
            for replica_id, h in self._hosts_ids_mapping.items():
                if replica_id == self._replica_id:
                    continue
                sock = ctx.socket(zmq.REQ)
                sock.RCVTIMEO = 1000
                sock.SNDTIMEO = 1000
                sock.connect(f'tcp://{h}:12345')
                res = zmq_retry_send(sock, {'type': 'victory', 'id': self._replica_id})
                if res:
                    _ = zmq_retry_recv(sock)

            election_state.set_leader_found(self._replica_id)

    def is_done(self):
        return self._done


class LeaderElectionListener(threading.Thread):
    def __init__(self, replica_id, bully_port=12345) -> None:
        super().__init__()
        self._replica_id = replica_id
        ctx = zmq.Context.instance()
        self._leader_election_channel = ctx.socket(zmq.REP)
        self._leader_election_channel.RCVTIMEO = 1000
        self._leader_election_channel.SNDTIMEO = 1000
        self._leader_election_channel.bind(f'tcp://*:{bully_port}')
        self._leader_election_triggers = []

    def run(self):
        # TODO: unhardcode messages
        while True:
            logging.info("Awaiting leader election msg")

            def dead_leader_election_triggers(trigger):
                if trigger.is_done():
                    trigger.join()
                    return False
                return True

            # garbage collection
            self._leader_election_triggers = list(
                filter(dead_leader_election_triggers, self._leader_election_triggers))
            try:
                res = self._leader_election_channel.recv_json()
            except zmq.error.Again:
                continue
            except BaseException as e:
                raise e

            if res['type'] == 'election':
                challenger_id = res['id']
                # no debería pasar esto, pero anyway...
                if challenger_id > self._replica_id:
                    continue
                zmq_retry_send(self._leader_election_channel, {'type': 'answer'})
                t = LeaderElectionTrigger(
                    replica_id=self._replica_id,
                    hosts_ids_mapping={0: 'bully0', 1: 'bully1', 2: 'bully2'},
                )
                t.start()
                self._leader_election_triggers.append(t)
            elif res['type'] == 'answer':
                self._leader_election_channel.send_json({'type': 'PING'})
            elif res['type'] == 'victory':
                self._leader_election_channel.send_json({'type': 'PING'})
                ElectionStateMonitor().set_leader_found(res['id'])
                logging.info(f'[{os.getenv("REPLICA_ID")}] leader is: {res["id"]}')


class Healthchecker(threading.Thread):
    def __init__(self, remote_host, port):
        super().__init__()
        self._remote_host = remote_host
        self._port = port
        ctx = zmq.Context.instance()
        self._socket = ctx.socket(zmq.REQ)
        # TODO: config vars for timeout
        self._socket.RCVTIMEO = 1000
        self._socket.SNDTIMEO = 1000

    def run(self) -> None:
        ping_msg = {'type': 'PING'}
        while True:
            sent = zmq_retry_send(self._socket, ping_msg)
            if not sent:
                print("SENT FAILED")
                continue
            recv = zmq_retry_recv(self._socket)
            if recv['type'] != 'PING':
                raise


class HealthcheckResponder(threading.Thread):
    def __init__(self):
        super().__init__()
        ctx = zmq.Context.instance()
        self._socket = ctx.socket(zmq.REP)
        self._socket.RCVTIMEO = 1000
        self._socket.SNDTIMEO = 1000

    def run(self) -> None:
        while True:
            recv = zmq_retry_recv(self._socket)
            if not recv:
                print("err")
                continue
            zmq_retry_send(self._socket, recv)


if __name__ == '__main__':
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    logging.info('RUNNING BULLY TEST')

    # bully system
    # TODO: stop hardcoding the ids to the host names
    leader_election = LeaderElectionTrigger(int(os.getenv('REPLICA_ID')), {0: 'bully0', 1: 'bully1', 2: 'bully2'})
    listener = LeaderElectionListener(replica_id=int(os.getenv('REPLICA_ID')))
    leader_election.start()
    listener.start()

    # aca de alguna manera hay que loopear
    # mientras no se sepa el lider, no se hace healthcheck
    # una vez conocido, se hace healthcheck
    # si llega a fallar el check, se lanza el hilo encargado de triggerear una eleccion nueva
    # hay que esperar a que el estado sea LeaderFound (y de alguna manera hay que comunicar quien es el lider)
    leader = ElectionStateMonitor().wait_leader_state()

    logging.info(f'got out of condition loop, leader is {leader}')

    leader_election.join()
    listener.join()
