#!/usr/bin/python3

import logging
from common_utils.utils import initialize_logging, parse_config
from common.client import Client
import datetime


def main():
    config_params = parse_config(server_addr='SERVER_ADDR',
                                 server_port='SERVER_PORT',
                                 data_path='DATA_PATH',
                                 output_path='OUTPUT_PATH',
                                 logging_level='LOGGING_LEVEL')
    initialize_logging(config_params['logging_level'])
    logging.debug(f'action: config | result: success | data_path: {config_params["data_path"]}')
    client = Client(
        server_addr=config_params['server_addr'],
        server_port=int(config_params['server_port']),
        data_path=config_params['data_path'],
        output_path=config_params['output_path'],
    )
    try:
        start = datetime.datetime.now()
        client.run()
        end = datetime.datetime.now()
        logging.info(f'action: run | status: success | start_time: {start} | end_time: {end} | rough_time_difference: {end - start}')
    except BaseException as e:
        if not client.closed:
            logging.error(f'action: run | status: failed | reason: {e}') 
            raise e
        else: 
            logging.info('action: stop | status: success | gracefully quitting')


if __name__ == '__main__':
    main()
