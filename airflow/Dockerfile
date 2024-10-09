# Use the official Airflow Python image as the base
FROM apache/airflow:slim-latest-python3.9

# Set the working directory inside the container
WORKDIR /opt/airflow

# Install necessary Python packages in one layer to reduce image size
RUN pip install \
    apache-airflow[apache-spark] \
    apache-airflow[hive] \
    sentry-sdk==0.10.2 \
    pyspark==3.5.2 \
    psycopg2-binary

RUN pip install --upgrade apache-airflow apache-airflow-providers-celery

# Copy required configuration files and directories into the container
COPY init-airflow.sh airflow.cfg /opt/airflow/
COPY dags /opt/airflow/dags
COPY logs /opt/airflow/logs
COPY config /opt/airflow/config
COPY plugins /opt/airflow/plugins
COPY pyfiles /opt/airflow/pyfiles

# Switch to root to install additional system packages
USER root

# Install OpenJDK 17 and Ant in a single RUN statement, cleaning up after installation to reduce image size
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Set the entrypoint to run the initialization script
ENTRYPOINT ["/opt/airflow/init-airflow.sh"]

# Expose the Airflow webserver port (optional)
EXPOSE 8080

# Default command to run the Airflow webserver
CMD ["webserver"]
