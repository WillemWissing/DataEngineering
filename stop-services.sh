#!/bin/bash

# =================================================
# Script to stop various Docker services for a data pipeline
# This script stops Kafka, Hadoop, Spark, Airflow, Hive, Trino, and Sentry services.
# =================================================

# Function to stop services and check for errors
stop_service() {
    local service_name=$1
    local compose_file=$2

    echo "Stopping $service_name services..."
    docker-compose -f "$compose_file" down -v
    if [ $? -ne 0 ]; then
        echo "Failed to stop $service_name services"
        exit 1
    fi
}

# Stop services
stop_service "Sentry" "sentry/docker-compose.yml"
stop_service "Trino" "trino/docker-compose.yml"
stop_service "Hive" "hive/docker-compose.yml"
stop_service "Airflow" "airflow/docker-compose.yml"
stop_service "Spark" "spark/docker-compose.yml"
stop_service "Hadoop" "hadoop/docker-compose.yml"
stop_service "Kafka" "kafka/docker-compose.yml"

# Remove Docker network "data-pipeline"
echo "Removing Docker network: data-pipeline"
docker network rm data-pipeline

echo "All services stopped successfully"
