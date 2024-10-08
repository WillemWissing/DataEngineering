from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sentry_sdk
import os

# Initialize Sentry in the DAG
def initialize_sentry():
    """
    Reads the Sentry DSN from a file and initializes the Sentry SDK.
    This setup allows capturing errors that occur in the DAG.
    """
    try:
        # Read DSN from file (ensure the path is accessible within the container)
        with open("/etc/spark/sentry_dsn.txt", "r") as file:
            dsn = file.read().strip()
        
        # Initialize Sentry with the retrieved DSN
        sentry_sdk.init(
            dsn=dsn,
            debug=True,  # Enable Sentry debug mode for more verbose logging
            shutdown_timeout=10  # Time to wait for Sentry to flush events before shutdown
        )
        print("Sentry successfully initialized with DSN:", dsn)

    except FileNotFoundError:
        print("Error: Sentry DSN file not found. Ensure the file exists at '/etc/spark/sentry_dsn.txt'.")
    except Exception as e:
        print(f"Error initializing Sentry: {e}")

# Call Sentry initialization
initialize_sentry()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Prevents tasks from depending on previous DAG runs
    'start_date': datetime(2024, 8, 1),  # Set the DAG start date
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retry
    'retries': 0,  # No retries in case of task failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries, unused as retries=0
}

# Define the DAG
dag = DAG(
    'spark_connection_test_dag',
    default_args=default_args,
    description='A simple DAG to test Spark connection',
    schedule_interval='@once',  # Run only once for connection testing
    catchup=False  # Do not catch up on missed runs
)

# Define the SparkSubmitOperator task
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_test_job',
    application='/opt/airflow/pyfiles/test_application.py',  # Path to the test Spark job script
    conn_id='spark_default',  # Airflow connection ID for Spark (must be pre-configured)
    verbose=True, # Enable verbose logging
    dag=dag  # Attach the task to the DAG
)

# Task ordering (in this case, just a single task)
spark_submit_task
