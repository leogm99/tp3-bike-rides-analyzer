import os
from typing import Any, Dict
from common.dag_node import DAGNode


def build_node(node_name: str, config_params: Dict[str, Any]) -> DAGNode:
    if node_name == 'LOADER':
        from common.loader.loader import Loader
        from common.loader.loader_middleware import LoaderMiddleware
        from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware
        from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
        middleware = LoaderMiddleware(
            hostname=config_params['rabbit_hostname'],
        )
        return Loader(
            port=int(config_params['port']),
            backlog=int(config_params['backlog']),
            stations_consumer_replica_count=int(config_params['stations_consumer_replicas']),
            weather_consumer_replica_count=int(config_params['weather_consumer_replicas']),
            trips_consumer_replica_count=int(config_params['trips_consumer_replicas']),
            ack_count=int(config_params['joiner_by_date_replicas']) + int(
                config_params['joiner_by_year_city_station_id_replicas']) + int(
                config_params['joiner_by_year_end_station_id_replicas']),
            middleware=middleware,
            hostname=config_params['rabbit_hostname'],
            max_clients=int(config_params['max_clients']),
        )
    elif node_name == 'TRIPS_CONSUMER':
        from common.consumers.trips_consumer.trips_consumer import TripsConsumer
        from common.consumers.trips_consumer.trips_consumer_middleware import TripsConsumerMiddleware
        middleware = TripsConsumerMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=1,
            node_id=int(os.getenv('ID', 0)),
        )
        return TripsConsumer(
            filter_by_city_consumers=int(config_params['filter_by_city_replicas']),
            filter_by_year_consumers=int(config_params['filter_by_year_replicas']),
            joiner_by_date_consumers=int(config_params['joiner_by_date_replicas']),
            middleware=middleware,
        )
    elif node_name == 'STATIONS_CONSUMER':
        from common.consumers.stations_consumer.stations_consumer import StationsConsumer
        from common.consumers.stations_consumer.stations_consumer_middleware import StationsConsumerMiddleware
        middleware = StationsConsumerMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=1,
            node_id=int(os.getenv('ID', 0)),
        )
        return StationsConsumer(
            filter_by_city_consumers=int(config_params['filter_by_city_replicas']),
            middleware=middleware,
        )
    elif node_name == 'WEATHER_CONSUMER':
        from common.consumers.weather_consumer.weather_consumer import WeatherConsumer
        from common.consumers.weather_consumer.weather_consumer_middleware import WeatherConsumerMiddleware
        middleware = WeatherConsumerMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=1,
            node_id=int(os.getenv('ID', 0)),
        )
        return WeatherConsumer(
            weather_consumers=int(config_params['filter_by_precipitation_replicas']),
            middleware=middleware,
        )
    elif node_name == 'FILTER_BY_PRECIPITATION':
        from common.filters.by_precipitation.filter_by_precipitation import FilterByPrecipitation
        from common.filters.by_precipitation.filter_by_precipitation_middleware import FilterByPrecipitationMiddleware
        middleware = FilterByPrecipitationMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['weather_consumer_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return FilterByPrecipitation(
            filter_key='prectot',
            low=30,
            high=float('inf'),
            middleware=middleware,
        )
    elif node_name == 'FILTER_BY_YEAR':
        from common.filters.by_year.filter_by_year import FilterByYear
        from common.filters.by_year.filter_by_year_middleware import FilterByYearMiddleware
        middleware = FilterByYearMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['trips_consumer_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return FilterByYear(
            filter_key='yearid',
            low=2016,
            high=2017,
            keep_filter_key=True,
            consumers=int(config_params['joiner_by_year_city_station_id_replicas']),
            middleware=middleware,
        )
    elif node_name == 'FILTER_BY_CITY':
        from common.filters.by_city.filter_by_city import FilterByCity
        from common.filters.by_city.filter_by_city_middleware import FilterByCityMiddleware
        middleware = FilterByCityMiddleware(
            hostname=config_params['rabbit_hostname'],
            trips_producers=int(config_params['trips_consumer_replicas']),
            stations_producers=int(config_params['stations_consumer_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return FilterByCity(
            filter_key='city',
            filter_value='montreal',
            trips_consumers=int(config_params['joiner_by_year_end_station_id_replicas']),
            middleware=middleware,
        )
    elif node_name == 'FILTER_BY_DISTANCE':
        from common.filters.by_distance.filter_by_distance import FilterByDistance
        from common.filters.by_distance.filter_by_distance_middleware import FilterByDistanceMiddleware
        middleware = FilterByDistanceMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['aggregate_trip_distance_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return FilterByDistance(
            filter_key='distance',
            low=6,
            high=float('inf'),
            keep_filter_key=True,
            middleware=middleware,
        )
    elif node_name == 'FILTER_BY_COUNT':
        from common.filters.by_count.filter_by_count import FilterByCount
        from common.filters.by_count.filter_by_count_middleware import FilterByCountMiddleware
        middleware = FilterByCountMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['aggregate_trip_count_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return FilterByCount(
            filter_key='year_2016',
            low=float('-inf'),
            high=float('inf'),
            keep_filter_key=True,
            middleware=middleware
        )

    elif node_name == 'JOINER_BY_DATE':
        from common.joiners.by_date.join_by_date import JoinByDate
        from common.joiners.by_date.join_by_date_middleware import JoinByDateMiddleware
        middleware = JoinByDateMiddleware(
            hostname=config_params['rabbit_hostname'],
            weather_producers=int(config_params['filter_by_precipitation_replicas']),
            trips_producers=int(config_params['trips_consumer_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return JoinByDate(
            index_key=('date', 'city',),
            consumers=int(config_params['aggregate_trip_duration_replicas']),
            middleware=middleware
        )
    elif node_name == 'JOINER_BY_YEAR_CITY_STATION_ID':
        from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id import JoinByYearCityStationId
        from common.joiners.by_year_city_station_id.joiner_by_year_city_station_id_middleware import \
            JoinByYearCityStationIdMiddleware
        middleware = JoinByYearCityStationIdMiddleware(
            hostname=config_params['rabbit_hostname'],
            stations_producers=int(config_params['stations_consumer_replicas']),
            trips_producers=int(config_params['filter_by_year_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return JoinByYearCityStationId(
            index_key=('code', 'city', 'yearid'),
            consumers=int(config_params['aggregate_trip_count_replicas']),
            middleware=middleware,
        )
    elif node_name == 'JOINER_BY_YEAR_END_STATION_ID':
        from common.joiners.by_year_end_station_id.join_by_year_end_station_id import JoinByYearEndStationId
        from common.joiners.by_year_end_station_id.join_by_year_end_station_id_middleware import \
            JoinByYearEndStationIdMiddleware
        middleware = JoinByYearEndStationIdMiddleware(
            hostname=config_params['rabbit_hostname'],
            stations_producers=int(config_params['filter_by_city_replicas']),
            trips_producers=int(config_params['filter_by_city_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )

        return JoinByYearEndStationId(
            index_key=('code', 'yearid'),
            consumers=int(config_params['haversine_applier_replicas']),
            middleware=middleware,
        )

    elif node_name == 'AGGREGATE_TRIP_DURATION':
        from common.aggregators.aggregate_trip_duration.aggregate_trip_duration import AggregateTripDuration
        from common.aggregators.aggregate_trip_duration.aggregate_trip_duration_middleware import \
            AggregateTripDurationMiddleware
        middleware = AggregateTripDurationMiddleware(
            hostname=config_params['rabbit_hostname'],
            aggregate_id=int(os.getenv('ID', 0)),
            producers=int(config_params['joiner_by_date_replicas']),
        )
        return AggregateTripDuration(
            aggregate_keys=('date',),
            average_key='duration_sec',
            middleware=middleware,
        )
    elif node_name == 'AGGREGATE_TRIP_COUNT':
        from common.aggregators.aggregate_trip_count.aggregate_trip_count import AggregateTripCount
        from common.aggregators.aggregate_trip_count.aggregate_trip_count_middleware import AggregateTripCountMiddleware
        middleware = AggregateTripCountMiddleware(
            hostname=config_params['rabbit_hostname'],
            aggregate_id=int(os.getenv('ID', 0)),
            producers=int(config_params['joiner_by_year_city_station_id_replicas']),
        )
        return AggregateTripCount(
            aggregate_keys=('name',),
            consumers=int(config_params['filter_by_count_replicas']),
            middleware=middleware,
        )
    elif node_name == 'AGGREGATE_TRIP_DISTANCE':
        from common.aggregators.aggregate_trip_distance.aggregate_trip_distance import AggregateTripDistance
        from common.aggregators.aggregate_trip_distance.aggregate_trip_distance_middleware import \
            AggregateTripDistanceMiddleware
        middleware = AggregateTripDistanceMiddleware(
            hostname=config_params['rabbit_hostname'],
            aggregate_id=int(os.getenv('ID', 0)),
            producers=int(config_params['haversine_applier_replicas']),
        )

        return AggregateTripDistance(
            aggregate_keys=('end_station_name',),
            average_key='distance',
            consumers=int(config_params['filter_by_distance_replicas']),
            middleware=middleware,
        )

    elif node_name == 'HAVERSINE_APPLIER':
        from common.appliers.haversine_applier.haversine_applier import HaversineApplier
        from common.appliers.haversine_applier.haversine_applier_middleware import HaversineApplierMiddleware
        middleware = HaversineApplierMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['joiner_by_year_end_station_id_replicas']),
            node_id=int(os.getenv('ID', 0)),
        )
        return HaversineApplier(
            start_latitude_key='start_station_latitude',
            start_longitude_key='start_station_longitude',
            end_latitude_key='end_station_latitude',
            end_longitude_key='end_station_longitude',
            consumers=int(config_params['aggregate_trip_distance_replicas']),
            middleware=middleware,
        )
    elif node_name == 'METRICS_CONSUMER':
        from common.consumers.metrics_consumer.metrics_consumer import MetricsConsumer
        from common.consumers.metrics_consumer.metrics_consumer_middleware import MetricsConsumerMiddleware
        middleware = MetricsConsumerMiddleware(
            hostname=config_params['rabbit_hostname'],
            producers=int(config_params['aggregate_trip_duration_replicas']) + int(
                config_params['filter_by_count_replicas']) + int(config_params['filter_by_distance_replicas']),
        )
        return MetricsConsumer(
            middleware=middleware,
        )
    raise ValueError("Unknown node name")
