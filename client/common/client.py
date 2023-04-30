import csv
import socket
import logging
import signal
import json
from multiprocessing import Process, Lock
from common.utils import get_file_paths_by_city_and_type
from typing import Dict
from common_utils.utils import send_string_message, receive_string_message, recv_n_bytes
from time import sleep

BATCH_SIZE = 10000


class Client:
    def __init__(self, server_addr: str, server_port: int, data_path: str):
        """
        :param server_addr: Server ip address.
        :param server_port: Server TCP listening port.
        :param data_path: Path to the data directory containing cities' data.
        """
        self._socket = socket.socket(family=socket.AF_INET,
                                     type=socket.SOCK_STREAM)
        self._data_path = data_path
        self._rwlock = Lock()
        try:
            logging.info('action: init_client | status: in progress')
            self._socket.connect((server_addr, server_port))
            logging.info('action: init_client | status: success')
        except socket.error as e:
            logging.error(f'action: init_client | status: failed | reason: {str(e)}')
            raise e
        signal.signal(signal.SIGTERM, lambda _: self.stop())

    def run(self):
        logging.info('action: register_sigterm | status: success')
        files_paths_by_city_and_type = get_file_paths_by_city_and_type(self._data_path)

        weather_sender = Process(target=self.__send_csv_data, args=(files_paths_by_city_and_type, 'weather',))
        stations_sender = Process(target=self.__send_csv_data, args=(files_paths_by_city_and_type, 'stations',))

        logging.debug(f'action: sending_static_data | status: in progress')
        weather_sender.start()
        stations_sender.start()
        weather_sender.join()
        stations_sender.join()
        logging.debug(f'action: sending_static_data | status: success')
        message = receive_string_message(recv_n_bytes, self._socket, 4)
        logging.info(f'action: receive-message | message: {message}')
        self.__send_csv_data(files_paths_by_city_and_type, 'trips')
        sleep(100)
        self.stop()

    def __send_csv_data(self, paths_by_city_and_type: Dict[str, Dict[str, str]], data_type: str):
        send_buffer = [None] * BATCH_SIZE
        current_packet = 0
        for city, city_paths in paths_by_city_and_type.items():
            if city != 'washington':
                continue
            data_path = city_paths[data_type]
            with open(data_path, newline='') as source:
                reader = csv.DictReader(f=source)
                for row in reader:
                    row['city'] = city
                    send_buffer[current_packet] = row
                    current_packet += 1
                    if current_packet == BATCH_SIZE:
                        message = {'type': data_type, 'payload': send_buffer}
                        json_message = json.dumps(message)
                        # throttle
                        sleep(0.001)
                        send_string_message(self.__send_all, json_message, 4)
                        current_packet = 0
                if current_packet != 0:
                    message = {'type': data_type, 'payload': send_buffer[:current_packet]}
                    json_message = json.dumps(message)
                    # throttle
                    # sleep(0.0001)
                    send_string_message(self.__send_all, json_message, 4)
        eof_data = {'type': data_type, 'payload': 'EOF'}
        json_eof_data = json.dumps(eof_data)
        send_string_message(self.__send_all, json_eof_data, 4)

    def __send_all(self, payload):
        """Process safe send_all"""
        with self._rwlock:
            self._socket.sendall(payload)

    def stop(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
