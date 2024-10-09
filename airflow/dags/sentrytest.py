import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sentry_sdk

# Initialize Sentry in DAG
def initialize_sentry():
    """
    Initializes Sentry using the DSN stored in a file.
    This setup allows Sentry to capture and report errors.
    """
    try:
        # Read DSN from a shared volume file
        with open("/etc/spark/sentry_dsn.txt", "r") as file:
            dsn = file.read().strip()

        # Initialize Sentry with the DSN
        sentry_sdk.init(
            dsn=dsn,
            debug=True,  # Enables debug mode for more detailed error logs
            shutdown_timeout=10  # Time to wait before Sentry flushes events on shutdown
        )
        print("Sentry initialized with DSN:", dsn)

    except FileNotFoundError:
        print("Error: Sentry DSN file not found. Ensure the file exists at '/etc/spark/sentry_dsn.txt'.")
    except Exception as e:
        print(f"Error initializing Sentry: {e}")

# Call Sentry initialization
initialize_sentry()

# Function to test Sentry in the DAG
def test_sentry_in_dag():
    """
    Sends a test message and exception to Sentry to verify the integration.
    """
    try:
        # Log a test message to confirm Sentry connection
        sentry_sdk.capture_message("Sentry is successfully connected from Airflow DAG!")
        
        # Simulate an error to test Sentry's exception capturing
        raise ValueError("This is a test error from the Airflow DAG!")
    
    except Exception as e:
        # Capture and send the exception to Sentry
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()  # Ensure events are sent to Sentry before continuing
        print(f"Exception captured in DAG: {e}")

# Call the Sentry test function
test_sentry_in_dag()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Prevents tasks from depending on past DAG runs
    'start_date': datetime(2024, 9, 24),  # Start date for the DAG
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retry
    'retries': 1,  # Number of retry attempts
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG with appropriate parameters
dag = DAG(
    'spark_sentry_integration_dag',
    default_args=default_args,
    description='A DAG to run a Spark job with Sentry integration',
    schedule_interval='@once',  # Run the DAG once; can be modified to fit other schedules
    catchup=False  # Do not run missed tasks from previous DAG executions
)

# Task: Submit the Spark job using SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/etc/spark/sparkErrorJob.py',  # Path to the Spark job script
    conn_id='spark_default',  # Airflow connection ID for Spark
    packages='org.apache.spark:spark-avro_2.12:3.5.2',  # Specify any additional Spark packages
    executor_memory='2g',  # Configure executor memory allocation
    driver_memory='2g',  # Configure driver memory allocation
    name='spark_job_with_sentry',  # Name of the Spark job
    verbose=True,  # Enable verbose output for troubleshooting
    conf={
        "spark.yarn.submit.waitAppCompletion": "true",  # Ensure the task waits for Spark job completion
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",  # Adjust the HDFS namenode connection
    },
    dag=dag  # Attach the task to the DAG
)

