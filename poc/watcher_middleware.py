import socket

WATCHER_PORT = 12346
RETRIES = 3
WATCHER_RETRY_RATE = 1


class WatcherMiddleware:
    def __init__(self, bind=False):
        self._udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        if bind:
            self._udp_socket.bind(('', WATCHER_PORT))
        self._udp_socket.settimeout(WATCHER_RETRY_RATE)

    def receive_watcher_message(self):
        retries = 3
        while retries:
            try:
                _, addr = self.__recv_all(1)
                return addr
            except BaseException:
                retries -= 1
        return None

    def send_watcher_message(self, addr):
        retries = 3
        while retries:
            try:
                self.__send_all(b'\x00', (addr, WATCHER_PORT))
                return True
            except BaseException:
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
        if len(payload) == 1:
            self._udp_socket.sendto(payload, addr)
            return
        while sent != len(payload):
            sent += self._udp_socket.sendto(payload[sent:], addr)


    def close(self):
        self._udp_socket.close()
