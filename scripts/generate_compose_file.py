#!/usr/bin/env python3
import argparse

base_definitions = lambda replica_dict: f'''
version: '3.9'

x-node: &node
  entrypoint: python /main.py
  links:
    - rabbit
  depends_on:
    - rabbit
  volumes:
    - type: bind
      source: ./server/config.ini
      target: /config.ini
  restart: on-failure

services:
  rabbit:
    container_name: rabbit
    build:
      context: ./rabbit
      dockerfile: Dockerfile
    ports:
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check-port-connectivity"]
      interval: 10s
      timeout: 5s
      retries: 5

  loader:
    <<: *node
    container_name: loader
    image: loader:latest
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - NODE_NAME=LOADER
      - STATIONS_CONSUMER_REPLICAS={replica_dict['stations_consumer']}
      - WEATHER_CONSUMER_REPLICAS={replica_dict['weather_consumer']}
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - JOINER_BY_DATE_REPLICAS={replica_dict['joiner_by_date']}
      - JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS={replica_dict['joiner_by_year_city_station_id']}
      - JOINER_BY_YEAR_END_STATION_ID_REPLICAS={replica_dict['joiner_by_year_end_station_id']}
      - MAX_CLIENTS={replica_dict['max_clients']}
      - PORT=8888
    ports:
      - "8888:8888"
    depends_on:
      - trips_consumer
      - stations_consumer
      - weather_consumer

  trips_consumer:
    <<: *node
    image: trips_consumer:latest
    entrypoint: python /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
      - NODE_NAME=TRIPS_CONSUMER
      - FILTER_BY_CITY_REPLICAS={replica_dict['filter_by_city']}
      - FILTER_BY_YEAR_REPLICAS={replica_dict['filter_by_year']}
      - JOINER_BY_DATE_REPLICAS={replica_dict['joiner_by_date']}
    deploy:
      mode: replicated
      replicas: {replica_dict['trips_consumer']}
    depends_on:
      - filter_by_city
      - filter_by_year
      - joiner_by_date
      
  metrics_consumer:
    <<: *node
    image: metrics_consumer:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=METRICS_CONSUMER
      - FILTER_BY_COUNT_REPLICAS={replica_dict['filter_by_count']}
      - FILTER_BY_DISTANCE_REPLICAS={replica_dict['filter_by_distance']}
      - AGGREGATE_TRIP_DURATION_REPLICAS={replica_dict['aggregate_trip_duration']}
    depends_on:
      - rabbit 

  stations_consumer:
    <<: *node
    image: stations_consumer:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=STATIONS_CONSUMER
      - FILTER_BY_CITY_REPLICAS={replica_dict['filter_by_city']}
    deploy:
      mode: replicated
      replicas: {replica_dict['stations_consumer']}
    depends_on:
      - filter_by_city
      - joiner_by_year_city_station_id

  weather_consumer:
    <<: *node
    image: weather_consumer:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=WEATHER_CONSUMER
      - FILTER_BY_PRECIPITATION_REPLICAS={replica_dict['filter_by_precipitation']}
    deploy:
      mode: replicated
      replicas: {replica_dict['weather_consumer']}
    depends_on:
      - filter_by_precipitation

  filter_by_year:
    <<: *node
    image: filter_by_year:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_YEAR
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS={replica_dict['joiner_by_year_city_station_id']}
    deploy:
      mode: replicated
      replicas: {replica_dict['filter_by_year']}
    depends_on:
      - joiner_by_year_city_station_id

  filter_by_distance:
    <<: *node
    image: filter_by_distance:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_DISTANCE
      - AGGREGATE_TRIP_DISTANCE_REPLICAS={replica_dict['aggregate_trip_distance']}
    deploy:
      mode: replicated
      replicas: {replica_dict['filter_by_distance']}
    depends_on:
      - rabbit

  filter_by_precipitation:
    <<: *node
    image: filter_by_precipitation:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_PRECIPITATION
      - WEATHER_CONSUMER_REPLICAS={replica_dict['weather_consumer']}
    deploy:
      mode: replicated
      replicas: {replica_dict['filter_by_precipitation']}
    depends_on:
      - joiner_by_date

  filter_by_city:
    <<: *node
    image: filter_by_city:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_CITY
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - JOINER_BY_YEAR_END_STATION_ID_REPLICAS={replica_dict['joiner_by_year_end_station_id']}
      - STATIONS_CONSUMER_REPLICAS={replica_dict['stations_consumer']}
    deploy:
      mode: replicated
      replicas: {replica_dict['filter_by_city']}
    depends_on:
      - joiner_by_year_end_station_id

  filter_by_count:
    <<: *node
    image: filter_by_count:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_COUNT
      - AGGREGATE_TRIP_COUNT_REPLICAS={replica_dict['aggregate_trip_count']}
    deploy:
      mode: replicated
      replicas: {replica_dict['filter_by_count']}
    depends_on:
      - metrics_consumer

  joiner_by_year_end_station_id:
    <<: *node
    image: joiner_by_year_end_station_id:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_YEAR_END_STATION_ID
      - FILTER_BY_CITY_REPLICAS={replica_dict['filter_by_city']}
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
    deploy:
      mode: replicated
      replicas: {replica_dict['joiner_by_year_end_station_id']}
    depends_on:
      - haversine_applier
      

'''

def joiner_by_date(replica_dict):
  definition = f'''
  joiner_by_date:
    <<: *node
    image: joiner_by_date:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_DATE
      - FILTER_BY_PRECIPITATION_REPLICAS={replica_dict['filter_by_precipitation']}
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - AGGREGATE_TRIP_DURATION_REPLICAS={replica_dict['aggregate_trip_duration']}
    deploy:
      mode: replicated
      replicas: {replica_dict['joiner_by_date']}
    depends_on:
  '''
  replicas = [f'\n      - aggregate_trip_duration_{i}' for i in range(replica_dict['aggregate_trip_duration'])]
  definition += ''.join(replicas)
  return definition

def joiner_by_year_city_station_id(replica_dict):
  definition = f'''
  joiner_by_year_city_station_id:
    <<: *node
    image: joiner_by_year_city_station_id:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_YEAR_CITY_STATION_ID
      - STATIONS_CONSUMER_REPLICAS={replica_dict['stations_consumer']}
      - FILTER_BY_YEAR_REPLICAS={replica_dict['filter_by_year']}
      - AGGREGATE_TRIP_COUNT_REPLICAS={replica_dict['aggregate_trip_count']}
    deploy:
      mode: replicated
      replicas: {replica_dict['joiner_by_year_city_station_id']}
    depends_on:
'''

  replicas = [f'\n      - aggregate_trip_count_{i}' for i in range(replica_dict['aggregate_trip_count'])]
  definition += ''.join(replicas)
  return definition

def haversine_applier(replica_dict):
  definition = f'''
  haversine_applier:
    <<: *node
    image: haversine_applier:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=HAVERSINE_APPLIER
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
      - JOINER_BY_YEAR_END_STATION_ID_REPLICAS={replica_dict['joiner_by_year_end_station_id']}
      - AGGREGATE_TRIP_DISTANCE_REPLICAS={replica_dict['aggregate_trip_distance']}
    deploy:
      mode: replicated
      replicas: {replica_dict['haversine_applier']}
    depends_on:'''
  replicas = [f'\n      - aggregate_trip_distance_{i}' for i in range(replica_dict['aggregate_trip_distance'])]
  definition += ''.join(replicas)
  return definition
      


aggregate_trip_duration = lambda replicas, replica_dict: [f'''
  aggregate_trip_duration_{n}:
    <<: *node
    image: aggregate_trip_duration:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=AGGREGATE_TRIP_DURATION
      - JOINER_BY_DATE_REPLICAS={replica_dict['joiner_by_date']}
      - ID={n}
    depends_on:
      - metrics_consumer
''' for n in range(replicas)]

aggregate_trip_distance = lambda replicas, replica_dict: [f'''
  aggregate_trip_distance_{n}:
    <<: *node
    image: aggregate_trip_distance:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=AGGREGATE_TRIP_DISTANCE
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
      - FILTER_BY_DISTANCE_REPLICAS={replica_dict['filter_by_distance']}
      - ID={n}
    depends_on:
      - filter_by_distance
''' for n in range(replicas)]

aggregate_trip_count = lambda replicas, replica_dict: [f'''
  aggregate_trip_count_{n}:
    <<: *node
    image: aggregate_trip_count:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=AGGREGATE_TRIP_COUNT
      - JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS={replica_dict['joiner_by_year_city_station_id']}
      - FILTER_BY_COUNT_REPLICAS={replica_dict['filter_by_count']}
      - ID={n}
    depends_on:
      - filter_by_count
''' for n in range(replicas)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trips_consumer', type=int, help='trips consumer replicas', default=1)
    parser.add_argument('--stations_consumer', type=int, help='stations consumer replicas', default=1)
    parser.add_argument('--weather_consumer', type=int, help='weather consumer replicas', default=1)
    parser.add_argument('--filter_by_year', type=int, help='filter by year replicas', default=1)
    parser.add_argument('--filter_by_distance', type=int, help='filter by distance replicas', default=1)
    parser.add_argument('--filter_by_precipitation', type=int, help='filter by precipitation replicas', default=1)
    parser.add_argument('--filter_by_city', type=int, help='filter by city replicas', default=1)
    parser.add_argument('--filter_by_count', type=int, help='filter by count replicas', default=1)
    parser.add_argument('--joiner_by_date', type=int, help='joiner by date replicas', default=1)
    parser.add_argument('--joiner_by_year_city_station_id', type=int,
                        help='joiner by year, city and station id replicas', default=1)
    parser.add_argument('--joiner_by_year_end_station_id', type=int,
                        help='joiner by year and end station id replicas', default=1)
    parser.add_argument('--aggregate_trip_duration', type=int,
                        help='aggregate trip duration replicas', default=1)
    parser.add_argument('--aggregate_trip_count', type=int,
                        help='aggregate trip count replicas', default=1)
    parser.add_argument('--aggregate_trip_distance', type=int,
                        help='aggregate trip distance replicas', default=1)
    parser.add_argument('--haversine_applier', type=int,
                        help='haversine applier replicas', default=1)
    parser.add_argument('--max_clients', type=int,
                        help='max clients in the system', default=1)

    args = parser.parse_args()
    replica_dict = {arg: getattr(args, arg) for arg in vars(args)}
    base = base_definitions(replica_dict)
    res = "".join((base,
                   *aggregate_trip_distance(replica_dict['aggregate_trip_distance'], replica_dict),
                   *aggregate_trip_duration(replica_dict['aggregate_trip_duration'], replica_dict),
                   *aggregate_trip_count(replica_dict['aggregate_trip_count'], replica_dict),
                   *haversine_applier(replica_dict),
                   *joiner_by_year_city_station_id(replica_dict),
                   *joiner_by_date(replica_dict)))
    with open('docker-compose.yml', 'w') as f:
        f.write(res)


if __name__ == '__main__':
    main()
