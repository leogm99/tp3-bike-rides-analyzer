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
'''

def loader(replica_dict):
  definition = f'''
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
    depends_on:'''
  replicas = [f'\n      - trips_consumer_{j}' for j in range(replica_dict['trips_consumer'])]
  replicas.extend([f'\n      - stations_consumer_{j}' for j in range(replica_dict['stations_consumer'])])
  replicas.extend([f'\n      - weather_consumer_{j}' for j in range(replica_dict['weather_consumer'])])
  definition += ''.join(replicas)
  return definition + '\n'

def trips_consumer(replica_dict):
  definitions = ''
  for i in range(replica_dict['trips_consumer']):
    definition = f'''
  trips_consumer_{i}:
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
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - filter_by_city_{j}' for j in range(replica_dict['filter_by_city'])]
    replicas.extend([f'\n      - filter_by_year_{j}' for j in range(replica_dict['filter_by_year'])])
    replicas.extend([f'\n      - joiner_by_date_{j}' for j in range(replica_dict['joiner_by_date'])])
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'


def stations_consumer(replica_dict):
  definitions = ''
  for i in range(replica_dict['stations_consumer']):
    definition = f'''
  stations_consumer_{i}:
    <<: *node
    image: stations_consumer:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=STATIONS_CONSUMER
      - FILTER_BY_CITY_REPLICAS={replica_dict['filter_by_city']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - filter_by_city_{j}' for j in range(replica_dict['filter_by_city'])]
    replicas.extend([f'\n      - joiner_by_year_city_station_id_{j}' for j in range(replica_dict['joiner_by_year_city_station_id'])])
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'


def weather_consumer(replica_dict):
  definitions = ''
  for i in range(replica_dict['weather_consumer']):
    definition = f'''
  weather_consumer_{i}:
    <<: *node
    image: weather_consumer:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=WEATHER_CONSUMER
      - FILTER_BY_PRECIPITATION_REPLICAS={replica_dict['filter_by_precipitation']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - filter_by_precipitation_{j}' for j in range(replica_dict['filter_by_precipitation'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'

def filter_by_year(replica_dict):
  definitions = ''
  for i in range(replica_dict['filter_by_year']):
    definition = f'''
  filter_by_year_{i}:
    <<: *node
    image: filter_by_year:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_YEAR
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS={replica_dict['joiner_by_year_city_station_id']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - joiner_by_year_city_station_id_{j}' for j in range(replica_dict['joiner_by_year_city_station_id'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'



def filter_by_distance(replica_dict):
  definitions = ''
  for i in range(replica_dict['filter_by_distance']):
    definition = f'''
  filter_by_distance_{i}:
    <<: *node
    image: filter_by_distance:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_DISTANCE
      - AGGREGATE_TRIP_DISTANCE_REPLICAS={replica_dict['aggregate_trip_distance']}
      - ID={i}
    depends_on:
      - rabbit'''
    definitions += definition
  return definitions + '\n'

def filter_by_precipitation(replica_dict):
  definitions = ''
  for i in range(replica_dict['filter_by_precipitation']):
    definition = f'''
  filter_by_precipitation_{i}:
    <<: *node
    image: filter_by_precipitation:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_PRECIPITATION
      - WEATHER_CONSUMER_REPLICAS={replica_dict['weather_consumer']}
      - ID={i}
    depends_on:  '''
    replicas = [f'\n      - joiner_by_date_{j}' for j in range(replica_dict['joiner_by_date'])]
    definition += ''.join(replicas)
    definitions += definition

  return definitions + '\n'

def filter_by_city(replica_dict):
  definitions = ''
  for i in range(replica_dict['filter_by_city']):
    definition = f'''
  filter_by_city_{i}:
    <<: *node
    image: filter_by_city:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_CITY
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - JOINER_BY_YEAR_END_STATION_ID_REPLICAS={replica_dict['joiner_by_year_end_station_id']}
      - STATIONS_CONSUMER_REPLICAS={replica_dict['stations_consumer']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - joiner_by_year_end_station_id_{j}' for j in range(replica_dict['joiner_by_year_end_station_id'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'


def filter_by_count(replica_dict):
  definitions = ''
  for i in range(replica_dict['filter_by_count']):
    definition = f'''
  filter_by_count_{i}:
    <<: *node
    image: filter_by_count:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=FILTER_BY_COUNT
      - AGGREGATE_TRIP_COUNT_REPLICAS={replica_dict['aggregate_trip_count']}
      - ID={i}
    depends_on:
      - metrics_consumer'''
    definitions += definition
  return definitions + '\n'


def joiner_by_year_end_station_id(replica_dict):
  definitions = ''
  for i in range(replica_dict['joiner_by_year_end_station_id']):
    definition = f'''
  joiner_by_year_end_station_id_{i}:
    <<: *node
    image: joiner_by_year_end_station_id:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_YEAR_END_STATION_ID
      - FILTER_BY_CITY_REPLICAS={replica_dict['filter_by_city']}
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - haversine_applier_{j}' for j in range(replica_dict['haversine_applier'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'
      

def joiner_by_date(replica_dict):
  definitions = ''
  for i in range(replica_dict['joiner_by_date']):
    definition = f'''
  joiner_by_date_{i}:
    <<: *node
    image: joiner_by_date:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_DATE
      - FILTER_BY_PRECIPITATION_REPLICAS={replica_dict['filter_by_precipitation']}
      - TRIPS_CONSUMER_REPLICAS={replica_dict['trips_consumer']}
      - AGGREGATE_TRIP_DURATION_REPLICAS={replica_dict['aggregate_trip_duration']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - aggregate_trip_duration_{i}' for i in range(replica_dict['aggregate_trip_duration'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'

def joiner_by_year_city_station_id(replica_dict):
  definitions = ''
  for i in range(replica_dict['joiner_by_year_city_station_id']):
    definition = f'''
  joiner_by_year_city_station_id_{i}:
    <<: *node
    image: joiner_by_year_city_station_id:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=JOINER_BY_YEAR_CITY_STATION_ID
      - STATIONS_CONSUMER_REPLICAS={replica_dict['stations_consumer']}
      - FILTER_BY_YEAR_REPLICAS={replica_dict['filter_by_year']}
      - AGGREGATE_TRIP_COUNT_REPLICAS={replica_dict['aggregate_trip_count']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - aggregate_trip_count_{i}' for i in range(replica_dict['aggregate_trip_count'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'

def haversine_applier(replica_dict):
  definitions = ''
  for i in range(replica_dict['haversine_applier']):
    definition = f'''
  haversine_applier_{i}:
    <<: *node
    image: haversine_applier:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=HAVERSINE_APPLIER
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
      - JOINER_BY_YEAR_END_STATION_ID_REPLICAS={replica_dict['joiner_by_year_end_station_id']}
      - AGGREGATE_TRIP_DISTANCE_REPLICAS={replica_dict['aggregate_trip_distance']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - aggregate_trip_distance_{i}' for i in range(replica_dict['aggregate_trip_distance'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'

aggregate_trip_duration = lambda replica_dict: [f'''
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
''' for n in range(replica_dict['aggregate_trip_duration'])]

def aggregate_trip_distance(replica_dict):
  definitions = ''
  for i in range(replica_dict['aggregate_trip_distance']):
    definition = f'''
  aggregate_trip_distance_{i}:
    <<: *node
    image: aggregate_trip_distance:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=AGGREGATE_TRIP_DISTANCE
      - HAVERSINE_APPLIER_REPLICAS={replica_dict['haversine_applier']}
      - FILTER_BY_DISTANCE_REPLICAS={replica_dict['filter_by_distance']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - filter_by_distance_{j}' for j in range(replica_dict['filter_by_distance'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'

def aggregate_trip_count(replica_dict):
  definitions = ''
  for i in range(replica_dict['aggregate_trip_count']):
    definition = f'''
  aggregate_trip_count_{i}:
    <<: *node
    image: aggregate_trip_count:latest
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_NAME=AGGREGATE_TRIP_COUNT
      - JOINER_BY_YEAR_CITY_STATION_ID_REPLICAS={replica_dict['joiner_by_year_city_station_id']}
      - FILTER_BY_COUNT_REPLICAS={replica_dict['filter_by_count']}
      - ID={i}
    depends_on:'''
    replicas = [f'\n      - filter_by_count_{j}' for j in range(replica_dict['filter_by_count'])]
    definition += ''.join(replicas)
    definitions += definition
  return definitions + '\n'


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
                   *aggregate_trip_distance(replica_dict),
                   *aggregate_trip_duration(replica_dict),
                   *aggregate_trip_count(replica_dict),
                   *haversine_applier(replica_dict),
                   *joiner_by_year_city_station_id(replica_dict),
                   *joiner_by_date(replica_dict),
                   *joiner_by_year_end_station_id(replica_dict),
                   *filter_by_count(replica_dict),
                   *filter_by_distance(replica_dict),
                   *filter_by_city(replica_dict),
                   *filter_by_precipitation(replica_dict),
                   *filter_by_year(replica_dict),
                   *weather_consumer(replica_dict),
                   *stations_consumer(replica_dict),
                   *trips_consumer(replica_dict),
                   *loader(replica_dict),
                   ))
    with open('docker-compose.yml', 'w') as f:
        f.write(res)


if __name__ == '__main__':
    main()
