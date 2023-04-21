#!/usr/bin/env python3

import logging
from common_utils.utils import initialize_logging, parse_config


def main():
    config_params = parse_config(
        port='PORT',
        logging_level='LOGGING_LEVEL',
        backlog='BACKLOG',
        rabbit_hostname='RABBIT_HOSTNAME',
        app_entrypoint='APP_ENTRYPOINT',
    )
    initialize_logging(config_params['logging_level'])
    logging.debug(f'action: config | result: success | '
                  f'config_params: {config_params}')
    app_entrypoint = config_params['app_entrypoint']
    if app_entrypoint == 'Loader':
        from common.loader.loader import Loader
        loader = Loader(
            port=int(config_params['port']),
            backlog=int(config_params['backlog']),
            rabbit_hostname=config_params['rabbit_hostname'],
            weather_queue='',
            stations_queue='',
            trips_queue='',
            static_data_ack_queue='',
        )
        loader.run()


if __name__ == '__main__':
    main()
