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
            data_exchange='data',
            exchange_type='direct',
        )
        loader.run()
    elif app_entrypoint == 'Trips_Consumer':
        from common.consumers.trips_consumer.trips_consumer import TripsConsumer
        trips_consumer = TripsConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            data_exchange='data',
            exchange_type='direct',
            trips_queue_name='trips',
            mean_trip_time_joiner_exchange_name='mean_trip_time_joiner',
            mean_trip_time_joiner_exchange_type='fanout',
            trip_year_filter_routing_key='trip_year_filter',
            montreal_trips_filter_routing_key='montreal_trips_filter',
        )
        trips_consumer.run()
    elif app_entrypoint == 'Stations_Consumer':
        from common.consumers.stations_consumer.stations_consumer import StationsConsumer
        stations_consumer = StationsConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            data_exchange='data',
            exchange_type='direct',
            stations_queue_name='stations',
            duplicated_stations_departures_exchange_name='duplicated_stations_joiner',
            duplicated_stations_departures_exchange_type='fanout',
            montreal_stations_filter_routing_key='montreal_stations_filter',
        )
        stations_consumer.run()
    elif app_entrypoint == 'Weather_Consumer':
        from common.consumers.weather_consumer.weather_consumer import WeatherConsumer
        weather_consumer = WeatherConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            data_exchange='data',
            exchange_type='direct',
            weather_queue_name='weather',
            precipitation_filter_routing_key='precipitation_filter',
        )
        weather_consumer.run()
    elif app_entrypoint == 'Precipitation_Filter':
        from common.filters.precipitation_filter.precipitation_filter import PrecipitationFilter
        precipitation_filter = PrecipitationFilter(
            rabbit_hostname=config_params['rabbit_hostname'],
            queue_name='precipitation_filter',
        )
        precipitation_filter.run()
    elif app_entrypoint == 'Trip_Year_Filter':
        from common.filters.trip_year_filter.trip_year_filter import TripYearFilter
        trip_year_filter = TripYearFilter(
            rabbit_hostname=config_params['rabbit_hostname'],
            queue_name='trip_year_filter',
        )
        trip_year_filter.run()
    elif app_entrypoint == 'Montreal_Trips_Filter':
        from common.filters.montreal_trips_filter.montreal_trips_filter import MontrealTripsFilter
        montreal_trips_filter = MontrealTripsFilter(
            rabbit_hostname=config_params['rabbit_hostname'],
            queue_name='montreal_trips_filter',
        )
        montreal_trips_filter.run()
    elif app_entrypoint == 'Montreal_Stations_Filter':
        from common.filters.montreal_stations_filter.montreal_stations_filter import MontrealStationsFilter
        montreal_stations_filter = MontrealStationsFilter(
            rabbit_hostname=config_params['rabbit_hostname'],
            queue_name='montreal_stations_filter',
        )
        montreal_stations_filter.run()


if __name__ == '__main__':
    from time import sleep
    sleep(10)
    main()
