#!/bin/bash

# Docker cleanup script
docker system prune -f
docker network create kafka-network

# Kafka Message Scheduler - Main Application
rm -rf Scheduler/jobs_done_log.csv

# Message Data Generator - Mongo DB
rm -rf message-database/mongo

# Database Scheduler - MySQL DB
rm -rf database-scheduler/mysql

# Monitor logs
rm -rf monitoring/docker-monitor/logs/*
rm -rf monitoring/prometheus
