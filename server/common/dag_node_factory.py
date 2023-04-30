import os
from typing import Any, Dict
from common.dag_node import DAGNode


def build_node(node_name: str, config_params: Dict[str, Any]) -> DAGNode:
    if node_name == 'LOADER':
        from common.loader.loader import Loader
        return Loader(
            port=int(config_params['port']),
            backlog=int(config_params['backlog']),
            rabbit_hostname=config_params['rabbit_hostname'],
            stations_consumer_replica_count=int(config_params['stations_consumer_replicas']),
            weather_consumer_replica_count=int(config_params['weather_consumer_replicas']),
            trips_consumer_replica_count=int(config_params['trips_consumer_replicas']),
            ack_count=int(config_params['joiner_by_date_replicas']) + int(
                config_params['joiner_by_year_city_station_id_replicas'])
        )
    elif node_name == 'TRIPS_CONSUMER':
        from common.consumers.trips_consumer.trips_consumer import TripsConsumer
        return TripsConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            trips_producers=1,  # Loader
            filter_by_city_consumers=int(config_params['filter_by_city_replicas']),
            filter_by_year_consumers=int(config_params['filter_by_year_replicas']),
        )
    elif node_name == 'STATIONS_CONSUMER':
        from common.consumers.stations_consumer.stations_consumer import StationsConsumer
        return StationsConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            stations_producers=1,
            filter_by_city_consumers=int(config_params['filter_by_city_replicas']),
        )
    elif node_name == 'WEATHER_CONSUMER':
        from common.consumers.weather_consumer.weather_consumer import WeatherConsumer
        return WeatherConsumer(
            rabbit_hostname=config_params['rabbit_hostname'],
            weather_producers=1,
            weather_consumers=int(config_params['filter_by_precipitation_replicas']),
        )
    elif node_name == 'FILTER_BY_PRECIPITATION':
        from common.filters.by_precipitation.filter_by_precipitation import FilterByPrecipitation
        return FilterByPrecipitation(
            rabbit_hostname=config_params['rabbit_hostname'],
            filter_key='prectot',
            low=30,
            high=float('inf'),
            weather_producers=int(config_params['weather_consumer_replicas']),
        )
    elif node_name == 'FILTER_BY_YEAR':
        from common.filters.by_year.filter_by_year import FilterByYear
        return FilterByYear(
            filter_key='yearid',
            rabbit_hostname=config_params['rabbit_hostname'],
            low=2016,
            high=2017,
            keep_filter_key=True,
            producers=int(config_params['trips_consumer_replicas']),
        )
    elif node_name == 'FILTER_BY_CITY':
        from common.filters.by_city.filter_by_city import FilterByCity
        return FilterByCity(
            rabbit_hostname=config_params['rabbit_hostname'],
            filter_key='city',
            filter_value='montreal',
            trips_producers=int(config_params['trips_consumer_replicas']),
            stations_producers=int(config_params['stations_consumer_replicas']),
        )
    elif node_name == 'FILTER_BY_DISTANCE':
        from common.filters.by_distance.filter_by_distance import FilterByDistance
        return FilterByDistance(
            rabbit_hostname=config_params['rabbit_hostname'],
            filter_key='?',
            low=6,
            high=float('inf'),
            keep_filter_key=True,
        )
    elif node_name == 'JOINER_BY_DATE':
        from common.joiners.by_date.join_by_date import JoinByDate
        return JoinByDate(
            rabbit_hostname=config_params['rabbit_hostname'],
            index_key=('date', 'city',),
            weather_producers=int(config_params['filter_by_precipitation_replicas']),
            trips_producers=int(config_params['trips_consumer_replicas']),
            consumers=int(config_params['aggregate_trip_duration_replicas']),
        )
    elif node_name == 'JOINER_BY_YEAR_CITY_STATION_ID':
        from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id import JoinByYearCityStationId
        return JoinByYearCityStationId(
            rabbit_hostname=config_params['rabbit_hostname'],
            index_key=('code', 'city', 'yearid'),
            stations_producers=int(config_params['stations_consumer_replicas'])
        )
    elif node_name == 'AGGREGATE_TRIP_DURATION':
        from common.aggregators.aggregate_trip_duration.aggregate_trip_duration import AggregateTripDuration
        return AggregateTripDuration(
            rabbit_hostname=config_params['rabbit_hostname'],
            aggregate_keys=('date',),
            average_key='duration_sec',
            aggregate_id=int(os.getenv('ID', 0))
        )
    elif node_name == 'AGGREGATE_TRIP_COUNT':
        from common.aggregators.aggregate_trip_count.aggregate_trip_count import AggregateTripCount
        return AggregateTripCount(
            rabbit_hostname=config_params['rabbit_hostname'],
            aggregate_keys=('name', 'yearid'),
        )
    raise ValueError("Unknown node name")
