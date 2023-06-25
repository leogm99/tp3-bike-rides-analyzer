import threading
import copy
import logging

from common_utils.singleton import Singleton
from common.middleware.leader_election_middleware import LeaderElectionMiddleware, OK, COORDINATOR, ELECTION


ELECTION_TIMEOUT_SEC = 2

class LeaderId(metaclass=Singleton):
    def __init__(self):
        self._leader_id = None
        self._leader_lock = threading.Lock()

    def set(self, leader_id):
        with self._leader_lock:
            self._leader_id = leader_id

    def get(self):
        with self._leader_lock:
            return self._leader_id

    def nuke(self):
        with self._leader_lock:
            self._leader_id = None


class Ok(metaclass=Singleton):
    def __init__(self):
        self._ok_event = threading.Event()

    def set(self):
        self._ok_event.set()

    def wait(self, timeout):
        return self._ok_event.wait(timeout=timeout)

    def reset(self):
        self._ok_event.clear()


class LeaderElection:
    def __init__(self, my_id, id_host_mapping):
        self._my_id = my_id
        self._leader_id = LeaderId()
        self._ok = Ok()
        self._id_host_mapping = id_host_mapping
        self._comm = LeaderElectionMiddleware(id_host_mapping=self._id_host_mapping, listen=True)
        self._closed = threading.Event()
        self._elections = []

    def listen_messages(self):
        while not self._closed.is_set():
            try:
                message, sender_id = self._comm.recv_election_message()
                if self._closed.is_set():
                    logging.info('action: close | message: stopping leader election and shutting down')
                    break

                if not message:
                    continue

                if message == OK:
                    self._ok.set()
                elif message == ELECTION:
                    if self._my_id > sender_id:
                        self._comm.send_ok(self._my_id, sender_id)
                        self.run_election()
                elif message == COORDINATOR:
                    self._set_leader(sender_id)
                else:
                    # ?
                    pass
            except BaseException as e:
                if self._closed.is_set():
                    logging.info('action: close | message: stopping leader election and shutting down')
                    for election in self._elections:
                        election.join()
                    break
                else:
                    logging.error(f'action: leader_election | error: {e}')
                    raise e from e

    def stop(self):
        self._closed.set()
        self._comm.close()

    def run_election(self):
        t = threading.Thread(target=self.find_leader, args=(copy.deepcopy(self._id_host_mapping),))
        self._elections.append(t)
        t.start()

    def find_leader(self, id_host_mapping):
        comm = LeaderElectionMiddleware(id_host_mapping=self._id_host_mapping)

        self.nuke_leader_id()
        self._ok.reset()

        recipients = list(filter(lambda r: r > self._my_id, id_host_mapping.keys()))
        comm.send_election(self._my_id, recipients)

        if not self._ok.wait(timeout=ELECTION_TIMEOUT_SEC):
            comm.send_coordinator(self._my_id, id_host_mapping.keys())
            self.take_leadership()
        comm.close()

    def take_leadership(self):
        self._set_leader(self._my_id)

    def _set_leader(self, leader_id):
        self._leader_id.set(leader_id)

    def nuke_leader_id(self):
        self._leader_id.nuke()

    def get_leader_id(self):
        return self._leader_id.get()

    def am_i_leader(self):
        return self.get_leader_id() == self._my_id
