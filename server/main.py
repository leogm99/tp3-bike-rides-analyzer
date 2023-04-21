#!/usr/bin/env python3

import logging
from common_utils.utils import initialize_logging, parse_config


def main():
    config_params = parse_config(
        port='PORT',
        logging_level='LOGGING_LEVEL',
        backlog='BACKLOG',
        rabbit_hostname='RABBIT_HOSTNAME',
    )
    initialize_logging(config_params['logging_level'])
    logging.debug(f'action: config | result: success | '
                  f'port: {config_params["port"]} | '
                  f'logging_level: {config_params["logging_level"]}')


if __name__ == '__main__':
    main()
