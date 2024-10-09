#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Wait for the PostgreSQL server to be available before proceeding
until pg_isready -h airflow-postgres -U airflow; do
  echo "Waiting for airflow-PostgreSQL to start..."
  sleep 1  # Wait for 1 second before checking again
done

# Check if the Airflow metadata database is initialized; if not, initialize it
if ! airflow db check; then
  echo "Initializing the Airflow metadata database..."
  airflow db init 
fi

# Create an admin user with specified credentials
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Migrate the database to apply any changes
echo "Migrating the Airflow database..."
airflow db migrate

# Optionally, load example DAGs if needed (uncomment the line below to enable)
# airflow dags import <path-to-your-dag-file>

# Run the Python script to set up the Spark connection
echo "Running the Spark connection setup script..."
python /opt/airflow/pyfiles/setup_spark_connection.py

# Start Airflow with the provided command-line arguments
exec airflow "$@"
