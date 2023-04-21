import csv
import socket
import logging
import signal
import json
import struct
from multiprocessing import Process, Lock
from common.utils import get_file_paths_by_city_and_type
from typing import Dict
from common_utils.utils import receive_string_message, recv_n_bytes, send_string_message


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

    def run(self):
        signal.signal(signal.SIGTERM, lambda _: self.stop())
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
        self.stop()

    def __send_csv_data(self, paths_by_city_and_type: Dict[str, Dict[str, str]], data_type: str):
        for city, city_paths in paths_by_city_and_type.items():
            data_path = city_paths[data_type]
            with open(data_path, newline='') as source:
                reader = csv.DictReader(f=source)
                for row in reader:
                    # todo: consider encapsulating this inside a protocol module or similar
                    message = {'type': data_type, 'city': city, 'payload': row}
                    json_message = json.dumps(message)
                    send_string_message(self._socket.sendall, json_message, 4)
        eof_data = {'type': data_type, 'payload': "EOF"}
        json_eof_data = json.dumps(eof_data)
        send_string_message(self._socket.sendall, json_eof_data, 4)

    def stop(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
