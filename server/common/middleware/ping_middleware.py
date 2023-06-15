import socket
import struct

PING = b'p'
PAYLOAD_FORMAT = '!cb'
PING_PORT = 12345
RETRIES = 5
PING_RETRY_RATE = 1


class PingMiddleware:
    def __init__(self, send_port=PING_PORT, bind_port=PING_PORT):
        self._udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        if bind_port:
            self._udp_socket.bind(('', bind_port))
        self._udp_socket.settimeout(PING_RETRY_RATE)
        self._send_port = send_port

    def receive_ping(self):
        retries = 3
        while retries:
            try:
                data, _ = self.__recv_all(2)
                return struct.unpack(PAYLOAD_FORMAT, data)
            except BaseException as e:
                retries -= 1
        return None

    def send_ping(self, sender_id, addr):
        payload = struct.pack(PAYLOAD_FORMAT, PING, sender_id)
        retries = RETRIES
        while retries:
            try:
                self.__send_all(payload, (addr, self._send_port))
                return True
            except BaseException as e:
                retries -= 1
        return False

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
        while sent != len(payload):
            sent += self._udp_socket.sendto(payload[sent:], addr)


    def close(self):
        self._udp_socket.close()
