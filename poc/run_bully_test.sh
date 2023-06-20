#!/bin/sh

set -x

docker build -t "bully-poc" .

docker compose -f docker-compose.yml up -d
docker compose -f docker-compose.yml logs -f 
