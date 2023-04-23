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
	docker build -f ./server/common/filters/precipitation_filter/Dockerfile -t "precipitation_filter:latest" .
	docker build -f ./server/common/filters/trip_year_filter/Dockerfile -t "trip_year_filter:latest" .
	docker build -f ./server/common/filters/montreal_trips_filter/Dockerfile -t "montreal_trips_filter:latest" .
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