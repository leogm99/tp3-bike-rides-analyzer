#!/usr/bin/bash

 if [ $# -eq 0 ]; then
   echo "Need data folder as argument"
   exit 1
 fi

 DATA=$1


docker build -f ./client/Dockerfile -t "client:latest" .
docker run \
  -it \
  -e PYTHONUNBUFFERED=1 \
  -e LOGGING_LEVEL=DEBUG \
  -v $(pwd)/${DATA}:/data \
  -v $(pwd)/client/output/:/output \
  -v $(pwd)/client/config.ini:/config.ini \
  --restart=on-failure \
  --network=tp3-bike-rides-analyzer_default \
  client:latest \
  python main.py
