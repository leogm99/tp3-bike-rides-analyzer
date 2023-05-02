#!/usr/bin/env python3

import logging
from common_utils.utils import initialize_logging, parse_config
from common.dag_node_factory import build_node


def main():
    config_params = parse_config(
        port='PORT',
        logging_level='LOGGING_LEVEL',
        backlog='BACKLOG',
        rabbit_hostname='RABBIT_HOSTNAME',
        node_name='NODE_NAME',
        weather_consumer_replicas='WEATHER_CONSUMER_REPLICAS',
        stations_consumer_replicas='STATIONS_CONSUMER_REPLICAS',
        trips_consumer_replicas='TRIPS_CONSUMER_REPLICAS',
        filter_by_city_replicas='FILTER_BY_CITY_REPLICAS',
        filter_by_year_replicas='FILTER_BY_YEAR_REPLICAS',
        filter_by_precipitation_replicas='FILTER_BY_PRECIPITATION_REPLICAS',
        filter_by_distance_replicas='FILTER_BY_DISTANCE_REPLICAS',
        filter_by_count_replicas='FILTER_BY_COUNT_REPLICAS',
        joiner_by_date_replicas='JOINER_BY_DATE_REPLICAS',
        joiner_by_year_city_station_id_replicas='JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS',
        joiner_by_year_end_station_id_replicas='JOINER_BY_YEAR_END_STATION_ID_REPLICAS',
        aggregate_trip_duration_replicas='AGGREGATE_TRIP_DURATION_REPLICAS',
        aggregate_trip_count_replicas='AGGREGATE_TRIP_COUNT_REPLICAS',
        aggregate_trip_distance_replicas='AGGREGATE_TRIP_DISTANCE_REPLICAS',
        haversine_applier_replicas='HAVERSINE_APPLIER_REPLICAS'
    )
    initialize_logging(config_params['logging_level'])
    logging.debug(f'action: config | result: success | '
                  f'config_params: {config_params}')
    node_name = config_params['node_name']
    node = build_node(node_name, config_params)
    node.run()


if __name__ == '__main__':
    from time import sleep
    sleep(10)
    main()
