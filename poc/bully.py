import zmq
import threading
from enum import Enum


class ElectionState(Enum):
    UNKNOWN_LEADER = 0
    SEARCHING_LEADER = 1

class Watcher:
    def __init__(self, id) -> None:
        self._id = id

class LeaderElectionTrigger(threading.Thread):
    def __init__(self, id, hosts_ids_mapping, *args, **kwargs) -> None:
        super().__init__(args, kwargs)
        self._hosts_ids_mapping = hosts_ids_mapping
        self._id = id

    def run(self) -> None:
        ctx = zmq.Context.instance()
        results = []
        for id, h in self._hosts_ids_mapping.items():
            if id > self._id or id == self._id:
                continue
            sock = ctx.socket(socket_type=zmq.REQ)
            sock.connect(f'tcp://{h}:12345')
            # TODO: esto debería hacerse con un timeout y retry
            sock.send_json({'type': 'election', 'id': self._id})
            # TODO: idem anterior
            res = sock.recv_json()
            results.append(res)

        # si no hay resultados, significa que soy el lider porque nadie me respondio
        if not results:
            for h in self._hosts_ids_mapping.values():
                sock = ctx.socket(socket_type=zmq.REQ)
                sock.connect(f'tcp://{h}:12345')
                sock.send_json({'type': 'victory', 'id': self._id})

    def is_done(self):
        # TODO
        pass

class LeaderElectionListener:
    def __init__(self) -> None:
        super().__init__()
        PORT = 12345  # Bully port
        ctx = zmq.Context.instance()
        self._leader_election_channel = ctx.socket(socket_type=zmq.REP)
        self._leader_election_channel.bind(f'tcp://*:{PORT}')
        self._leader_election_triggers = []

    def run(self):
        while True:
            res = self._leader_election_channel.recv_json()
            # TODO
            # manejar segun el tipo de respuesta

            t = LeaderElectionTrigger()
            t.start()

            def dead_leader_election_triggers(t):
                if t.is_done():
                    t.join()
                    return False
                return True

            self._leader_election_triggers.append(t)
            self._leader_election_triggers = list(filter(dead_leader_election_triggers, self._leader_election_triggers))


class Leader:
    def __init__(self) -> None:
        super().__init__()

class Follower:
    def __init__(self) -> None:
        super().__init__()    

if __name__ == '__main__':
    print("hello")
