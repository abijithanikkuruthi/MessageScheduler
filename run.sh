#!/bin/bash

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

watch -n 5 --color docker logs messenger