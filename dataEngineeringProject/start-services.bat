@echo off
REM ==================================================
REM Script to start various Docker services for a data pipeline
REM This script creates a Docker network and starts 
REM Kafka, Hadoop, Spark, Airflow, Hive, Trino, and Sentry services.
REM ==================================================

REM Capture the start time of the script
set startTime=%time%

REM Create Docker network "data-pipeline" if it doesn't exist
docker network inspect data-pipeline >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo "Creating Docker network: data-pipeline"
    docker network create data-pipeline   
)

REM Start Hadoop services
docker-compose -f %~dp0hadoop/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Hadoop services"
    exit /b %ERRORLEVEL%
)

REM Start Kafka services
docker-compose -f %~dp0kafka/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Kafka services"
    exit /b %ERRORLEVEL%
)

REM Start Spark services
docker-compose -f %~dp0spark/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Spark services"
    exit /b %ERRORLEVEL%
)

REM Start Airflow services
docker-compose -f %~dp0airflow/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Airflow services"
    exit /b %ERRORLEVEL%
)

REM Start Hive services
docker-compose -f %~dp0hive/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Hive services"
    exit /b %ERRORLEVEL%
)

REM Start Trino services
docker-compose -f %~dp0trino/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Trino services"
    exit /b %ERRORLEVEL%
)

REM Start Sentry services
docker-compose -f %~dp0sentry/docker-compose.yml up -d --build
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to start Sentry services"
    exit /b %ERRORLEVEL%
)

REM All services started successfully
echo "All services started successfully"
echo Start Time: %startTime%
echo Finish Time: %time%
pause
