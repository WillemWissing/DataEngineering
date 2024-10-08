#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Create additional databases for Airflow and Sentry
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create databases for Airflow and Sentry
    CREATE DATABASE airflow;
    CREATE DATABASE sentry;

    -- Create users for Airflow and Sentry with specified passwords
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE USER sentry WITH PASSWORD 'sentry';

    -- Grant all privileges on the respective databases to the users
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE sentry TO sentry;
EOSQL

echo "Databases and users created successfully."
