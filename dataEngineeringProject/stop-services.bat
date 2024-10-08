@echo off

REM stop Sentry services
docker-compose -f %~dp0sentry/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Sentry services"
    exit /b %ERRORLEVEL%
)

REM stop Trino services
docker-compose -f %~dp0trino/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Trino services"
    exit /b %ERRORLEVEL%
)

REM stop Hive services
docker-compose -f %~dp0hive/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Hive services"
    exit /b %ERRORLEVEL%
)

REM stop Airflow services
docker-compose -f %~dp0airflow/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Airflow services"
    exit /b %ERRORLEVEL%
)

REM stop Spark services
docker-compose -f %~dp0spark/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Spark services"
    exit /b %ERRORLEVEL%
)

REM stop Hadoop services
docker-compose -f %~dp0hadoop/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Hadoop services"
    exit /b %ERRORLEVEL%
)

REM stop Kafka services
docker-compose -f %~dp0kafka/docker-compose.yml down -v
IF %ERRORLEVEL% NEQ 0 (
    echo "Failed to stop Kafka services"
    exit /b %ERRORLEVEL%
)

REM remove Docker network "data-pipeline" 
echo "Removing Docker network: data-pipeline"
docker network rm data-pipeline   

echo "All services stopped successfully"
