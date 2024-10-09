#!/bin/bash

# =================================================
# Script to start various Docker services for a data pipeline
# This script creates a Docker network and starts 
# Kafka, Hadoop, Spark, Airflow, Hive, Trino, and Sentry services.
# =================================================

# Capture the start time of the script
startTime=$(date +"%T")

# Create Docker network "data-pipeline" if it doesn't exist
if ! docker network inspect data-pipeline > /dev/null 2>&1; then
    echo "Creating Docker network: data-pipeline"
    docker network create data-pipeline
fi

# Function to start services and check for errors
start_service() {
    local service_name=$1
    local compose_file=$2

    echo "Starting $service_name services..."
    docker-compose -f "$compose_file" up -d --build
    if [ $? -ne 0 ]; then
        echo "Failed to start $service_name services"
        exit 1
    fi
}

# Start services
start_service "Hadoop" "hadoop/docker-compose.yml"
start_service "Kafka" "kafka/docker-compose.yml"
start_service "Spark" "spark/docker-compose.yml"
start_service "Airflow" "airflow/docker-compose.yml"
start_service "Hive" "hive/docker-compose.yml"
start_service "Trino" "trino/docker-compose.yml"
start_service "Sentry" "sentry/docker-compose.yml"

# All services started successfully
echo "All services started successfully"
echo "Start Time: $startTime"
echo "Finish Time: $(date +"%T")"
