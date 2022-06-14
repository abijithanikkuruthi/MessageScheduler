#!/bin/bash

export EXPERIMENT_DURATION_HOURS=1
export EXPERIMENT_MESSAGE_COUNT=10000
export MESSAGE_SIZE_BYTES=1024

# Docker cleanup script
docker system prune -f
docker network create kafka-network

cd kafka
docker-compose up --build -d

sleep 5

cd ..
docker-compose up --build -d

sleep 5

cd database-scheduler
docker-compose up --build -d

sleep 5

cd ../monitoring
docker-compose up --build -d

sleep 5

cd ../message-database
docker-compose up --build -d

sleep 5

cd ../performance-analysis
docker-compose up --build -d

watch -n 5 --color docker logs messenger