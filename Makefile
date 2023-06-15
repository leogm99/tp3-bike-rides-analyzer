SHELL := /bin/bash
PWD := $(shell pwd)

docker-image:
	docker build -f ./base-images/client.dockerfile -t "client:latest" .
	docker build -f ./base-images/server.dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .

	docker build -f ./server/common/loader/Dockerfile -t "loader:latest" .

	docker build -f ./server/common/consumers/trips_consumer/Dockerfile -t "trips_consumer:latest" .
	docker build -f ./server/common/consumers/stations_consumer/Dockerfile -t "stations_consumer:latest" .
	docker build -f ./server/common/consumers/weather_consumer/Dockerfile -t "weather_consumer:latest" .
	docker build -f ./server/common/consumers/metrics_consumer/Dockerfile -t "metrics_consumer:latest" .

	docker build -f ./server/common/filters/Dockerfile -t "filter:latest" .
	docker build -f ./server/common/filters/by_year/Dockerfile -t "filter_by_year:latest" .
	docker build -f ./server/common/filters/by_distance/Dockerfile -t "filter_by_distance:latest" .
	docker build -f ./server/common/filters/by_precipitation/Dockerfile -t "filter_by_precipitation:latest" .
	docker build -f ./server/common/filters/by_city/Dockerfile -t "filter_by_city:latest" .
	docker build -f ./server/common/filters/by_count/Dockerfile -t "filter_by_count:latest" .

	docker build -f ./server/common/joiners/Dockerfile -t "joiner:latest" .
	docker build -f ./server/common/joiners/by_date/Dockerfile -t "joiner_by_date:latest" .
	docker build -f ./server/common/joiners/by_year_city_station_id/Dockerfile -t "joiner_by_year_city_station_id:latest" .
	docker build -f ./server/common/joiners/by_year_end_station_id/Dockerfile -t "joiner_by_year_end_station_id:latest" .

	docker build -f ./server/common/aggregators/Dockerfile -t "aggregator:latest" .
	docker build -f ./server/common/aggregators/aggregate_trip_duration/Dockerfile -t "aggregate_trip_duration:latest" .
	docker build -f ./server/common/aggregators/aggregate_trip_count/Dockerfile -t "aggregate_trip_count:latest" .
	docker build -f ./server/common/aggregators/aggregate_trip_distance/Dockerfile -t "aggregate_trip_distance:latest" .

	docker build -f ./server/common/appliers/Dockerfile -t "applier:latest" .
	docker build -f ./server/common/appliers/haversine_applier/Dockerfile -t "haversine_applier:latest" .
	docker build -f ./server/common/watchers/Dockerfile -t "watcher:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f ./docker-compose.yml up --build -d
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f ./docker-compose.yml stop -t 1
	docker compose -f ./docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f ./docker-compose.yml logs -f
.PHONY: docker-compose-logs
