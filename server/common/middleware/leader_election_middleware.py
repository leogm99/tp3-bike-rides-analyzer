import logging
import socket
import struct

LEADER_ELECTION_PORT = 54321
RECV_TIMEOUT = 0.1

OK = b'o'
ELECTION = b'e'
COORDINATOR = b'c'
PAYLOAD_FORMAT = '!cb'


class LeaderElectionMiddleware:
    def __init__(self, id_host_mapping, listen=False):
        self._id_host_mapping = id_host_mapping
        self._udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        if listen:
            self._udp_socket.bind(('', LEADER_ELECTION_PORT))
        self._udp_socket.settimeout(RECV_TIMEOUT)

    def send_election(self, sender_id, recipients):
        payload = struct.pack(PAYLOAD_FORMAT, ELECTION, sender_id)
        for recipient in recipients:
            self.__send_all(payload, (self._id_host_mapping[recipient], LEADER_ELECTION_PORT))

    def send_ok(self, sender_id, recipient):
        payload = struct.pack(PAYLOAD_FORMAT, OK, sender_id)
        self.__send_all(payload, (self._id_host_mapping[recipient], LEADER_ELECTION_PORT))

    def send_coordinator(self, sender_id, recipients):
        payload = struct.pack(PAYLOAD_FORMAT, COORDINATOR, sender_id)
        for recipient in recipients:
            if recipient != sender_id:
                self.__send_all(payload, (self._id_host_mapping[recipient], LEADER_ELECTION_PORT))

    def recv_election_message(self):
        # every election message has two bytes
        try:
            data, addr = self.__recv_all(2)
            return struct.unpack(PAYLOAD_FORMAT, data)
        except socket.timeout:
            return None, None

    def close(self):
        self._udp_socket.close()

    def __recv_all(self, size):
        recvd = 0
        res = bytearray()
        addr = None
        while recvd != size:
            buff, addr = self._udp_socket.recvfrom(size - recvd)
            res.extend(buff)
            recvd += len(buff)
        return res, addr

    def __send_all(self, payload, addr):
        sent = 0
        try:
            while sent != len(payload):
                sent += self._udp_socket.sendto(payload[sent:], addr)
        except BaseException:
            return
