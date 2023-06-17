#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Need data folder as argument"
  exit 1
fi

DATA=$1

echo "version: '3'
services:
  client:
    container_name: client
    image: client:latest
    entrypoint: python /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    volumes:
      - type: bind
        source: ./${DATA}
        target: /data
      - type: bind
        source: ./client/output/
        target: /output
      - type: bind
        source: ./client/config.ini
        target: /config.ini
    restart: on-failure" > docker-compose-client.yml

docker build -f ./client/Dockerfile -t "client:latest" .
docker compose -f ./docker-compose-client.yml up --build -d
docker compose -f ./docker-compose-client.yml logs -f
