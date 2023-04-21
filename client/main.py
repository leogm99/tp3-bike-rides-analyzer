#!/usr/bin/python3

import logging
from common_utils.utils import initialize_logging, parse_config
from common.client import Client


def main():
    config_params = parse_config(server_addr='SERVER_ADDR',
                                 server_port='SERVER_PORT',
                                 data_path='DATA_PATH',
                                 logging_level='LOGGING_LEVEL')
    initialize_logging(config_params['logging_level'])
    logging.debug(f'action: config | result: success | data_path: {config_params["data_path"]}')
    client = Client(
        server_addr=config_params['server_addr'],
        server_port=int(config_params['server_port']),
        data_path=config_params['data_path'],
    )
    client.run()


if __name__ == '__main__':
    from time import sleep
    sleep(10)
    main()
