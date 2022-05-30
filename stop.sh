#!/bin/bash

cd monitoring
docker-compose down

cd ../message-database
docker-compose down

cd ../database-scheduler
docker-compose down

cd ..
docker-compose down

cd kafka
docker-compose down

sleep 2

# Docker cleanup script
docker system prune -f