import socket
import logging
import os
from configparser import ConfigParser


def initialize_logging(level):
    logging.getLogger('pika').propagate = False
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
            config_params[k] = os.getenv(v, config['DEFAULT'].get(v, ''))
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
    while current_received != n:
        bytes_recv = sock.recv_into(memoryview(recv_buffer)[current_received:])
        if bytes_recv == 0:
            raise BrokenPipeError
        current_received += bytes_recv
    return recv_buffer


def receive_string_message(recv_handle, sock, string_byte_length):
    import struct
    payload_size_buffer = recv_handle(sock, string_byte_length)
    format_char = ''
    if string_byte_length == 2:
        format_char = 'H'
    elif string_byte_length == 4:
        format_char = 'I'
    payload_size = struct.unpack(f'!{format_char}', payload_size_buffer)[0]
    string_payload = recv_handle(sock, payload_size)
    return struct.unpack(f'{payload_size}s', string_payload)[0].decode('utf8')


def send_string_message(send_handle, string_message, string_byte_length):
    import struct
    format_char = ''
    if string_byte_length == 2:
        format_char = 'H'
    elif string_byte_length == 4:
        format_char = 'I'
    string_encoded = string_message.encode('utf8')
    len_encoding = len(string_encoded)
    payload = struct.pack(f'!{format_char}{len_encoding}s', len_encoding, string_encoded)
    send_handle(payload)
