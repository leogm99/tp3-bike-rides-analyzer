SHELL := /bin/bash
PWD := $(shell pwd)

docker-image:
	docker build -f ./base-images/client.dockerfile -t "client:latest" .
	docker build -f ./base-images/server.dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./server/common/loader/Dockerfile -t "loader:latest" .
	docker build -f ./server/common/consumer/Dockerfile -t "trips_consumer:latest" .
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