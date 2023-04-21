import socket
import logging
import os
from configparser import ConfigParser


def initialize_logging(level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def parse_config(config_file: str = 'config.ini', **kwargs):
    config = ConfigParser(os.environ)
    config.read(config_file)
    config_params = {}
    try:
        # todo: type convert numeric values from strings
        for k, v in kwargs.items():
            config_params[k] = os.getenv(v, config['DEFAULT'][v])
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting.")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting.")
    return config_params


def recv_n_bytes(sock: socket.socket, n: int):
    """Receive loop to avoid short readings

    :param sock: TCP socket
    :param n: Amount of bytes to receive
    :return: Buffer with n bytes of data
    """
    if n <= 0:
        raise ValueError("Should be called with a positive amount of bytes to receive")
    recv_buffer = bytearray(n)
    current_received = 0
    while len(recv_buffer) != n:
        bytes_recv = sock.recv_into(memoryview(recv_buffer)[current_received:])
        if bytes_recv == 0:
            raise BrokenPipeError
        current_received += bytes_recv
    return recv_buffer
