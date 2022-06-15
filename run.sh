#!/bin/bash

export EXPERIMENT_DURATION_HOURS=6
export EXPERIMENT_MESSAGE_COUNT=10000000
export MESSAGE_SIZE_BYTES=10

export KAFKA_ENABLED=True
export DATABASE_SCHEDULER_CASSANDRA_ENABLED=False
export DATABASE_SCHEDULER_MYSQL_ENABLED=False

# Docker cleanup script
docker system prune -f
docker network create kafka-network

cd kafka
docker-compose up --build -d zookeeper kafka

sleep 5

cd ..
docker-compose up --build -d

sleep 5

cd database-scheduler
docker-compose up --build -d database-scheduler cassandra mysql

sleep 5

cd ../monitoring
docker-compose up --build -d docker-monitor

sleep 5

cd ../messenger
docker-compose up --build -d messenger mongo

sleep 5

cd ../performance-analysis
docker-compose up --build -d performance

watch -n 5 --color docker logs messenger