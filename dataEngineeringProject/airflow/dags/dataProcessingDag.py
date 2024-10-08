from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sentry_sdk
import os

# Initialize Sentry in DAG
def initialize_sentry():
    """
    Reads the Sentry DSN from a file and initializes Sentry SDK.
    This enables capturing and reporting errors during DAG execution.
    """
    try:
        # Read the DSN from a shared volume or config file
        with open("/etc/spark/sentry_dsn.txt", "r") as file:
            dsn = file.read().strip()

        # Initialize Sentry with the DSN and optional settings
        sentry_sdk.init(
            dsn=dsn,
            debug=True,  # Enable debugging logs for troubleshooting
            shutdown_timeout=10  # Time to wait for Sentry to flush events before shutdown
        )
        print("Sentry successfully initialized with DSN:", dsn)

    except FileNotFoundError:
        print("Error: Sentry DSN file not found. Ensure the file exists at '/etc/spark/sentry_dsn.txt'.")
    except Exception as e:
        print(f"Error initializing Sentry: {e}")

# Call the Sentry initialization function
initialize_sentry()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Don't run DAGs based on past runs
    'start_date': datetime(2024, 8, 1),  # Set a start date for the DAG schedule
    'email_on_failure': False,  # Disable email notifications on task failure
    'email_on_retry': False,  # Disable email notifications on task retry
    'retries': 0,  # No retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retries (not used here since retries=0)
}

# Define the DAG with appropriate parameters
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A DAG to run a Spark job periodically',
    schedule_interval='@once',  # Run the DAG once, adjust the interval as needed (e.g., daily/weekly)
    catchup=False  # Skip execution for past start_date runs
)

# Define the SparkSubmitOperator task
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='/etc/spark/hdfs_processing_avro_out.py',  # Path to your Spark job script
    conn_id='spark_default',  # The connection ID you created in Airflow for Spark
    packages='org.apache.spark:spark-avro_2.12:3.5.2',  # Spark Avro package dependency
    executor_memory='2g',  # Set the executor memory
    driver_memory='2g',  # Set the driver memory
    name='spark_job',  # Name of the Spark application
    verbose=True,
    conf={
        "spark.yarn.submit.waitAppCompletion": "true",
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000"  # Ensure default FS is HDFS
    },
    dag=dag
)

# Task ordering (since there's only one task, no need to explicitly set order)
spark_submit_task
