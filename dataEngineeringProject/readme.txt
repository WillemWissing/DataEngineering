Data Processing System Documentation

Overview
--------
This project consists of a set of microservices deployed in Docker containers, 
designed to process data using Apache Spark, Apache Kafka, Apache Airflow, 
Hive, Trino, and Sentry. The system ingests data through Kafka, processes it 
with Spark, and stores the results in Hadoop (HDFS). The integration includes 
error logging and monitoring via Sentry, workflow management through Airflow, 
and data querying using Hive and Trino.

Key Components:
- Apache Spark: Used for data processing.
- Apache Kafka: Ingests data into topics.
- Hadoop HDFS: Stores raw and processed data.
- Apache Hive: Manages structured data.
- Trino: Provides an SQL interface to query data.
- Apache Airflow: Manages workflows and pipelines.
- Sentry: Tracks errors and performance issues.

Service URLs
------------
- Sentry URL: http://localhost:9010/sentry/
- Airflow URL: http://localhost:8085/home
- Hadoop URL: http://localhost:9870/
- Spark URL: http://localhost:8080/

Services
--------
1. Kafka Connect: 
   - Automatically starts and reads data from the topic `data-ingestion-topic`. 
   - It writes the data to HDFS, using the schema provided by the Schema Registry service.

2. Airflow: 
   - Manages workflows through three pre-configured DAGs:
     - Testing Spark-Airflow connection.
     - Testing Sentry error logging by intentionally raising exceptions.
     - Running the Spark data processing job.

3. Sentry: 
   - Provides monitoring and error tracking. A default "internal" project is available, 
     but new projects can be created if needed.

Starting and Stopping Services
------------------------------
To start all services, run the following command:
   windows: start-services.bat
   linux: start-services.sh
To shut down all services, use:
    windows: stop-services.bat
    linux: stop-services.bat

The system will launch all required containers, and most services will start automatically. 
Note that Airflow and Sentry may take some time before their web interfaces become available.

- Airflow Web UI: Accessible at http://localhost:8085/home, login with `airflow/airflow`.
- Sentry Web UI: Accessible at http://localhost:9010/sentry/, login with `admin@example.com/mypassword`.



Updating Sentry DSN
-------------------
Once the Sentry service is running, retrieve the DSN from the Sentry web UI:

1. Navigate to Project -> Settings -> Client Keys/DSN.
2. Use the following command in a Spark container to update the shared DSN:
    echo "DSN" | tee /etc/spark/sentry_dsn.txt > /dev/null

This ensures that the Spark jobs log errors to Sentry.

Airflow DAGs
------------
There are three DAGs available in Airflow:

1. Spark Connection Test: 
   - Verifies the connection between Airflow and Spark.
2. Sentry Logging Test: 
   - Produces errors to validate Sentry's logging capabilities.
3. Data Processing Job: 
   - Runs the Spark job to process data from HDFS.

Data Processing Workflow
------------------------
The data processing job performs the following tasks:
- Reads data from HDFS.
- Filters the data using a basic operation.
- Writes the filtered data back to HDFS in a new directory.
- Moves the processed files to a designated folder for archived data.
- Empties the input directory.

Data Ingestion
--------------
To ingest data directly into the Kafka topic `data-ingestion-topic`, use the provided 
Python script `batch-data-producer.py`. This script reads data and publishes it to the Kafka topic.

Adjusting Batch Size
--------------------
- The batch size for producing data can be adjusted within the `batch-data-producer.py` script.
- Kafka Connect also has an adjustable batch size that can be configured to manage the data 
  flow from Kafka to HDFS.

Kafka Commands
--------------
- List Kafka topics:
    kafka-topics --list --bootstrap-server kafka:9092

- Start a Kafka console consumer:
    kafka-console-consumer --bootstrap-server kafka:9092 --topic data-ingestion-topic --from-beginning

- Start a Kafka console consumer with a limit on messages:
    kafka-console-consumer --bootstrap-server kafka:9092 --topic data-ingestion-topic --from-beginning --max-messages 10 --property print.key=true --property print.value=true

Connector Management
--------------------
- Create a new connector:
    curl -X POST -H "Content-Type: application/json" --data @/etc/kafka-connect/connect-hdfs-sink.json http://localhost:8083/connectors

- List existing connectors:
    curl -X GET http://localhost:8083/connectors

- Delete a connector:
    curl -X DELETE http://localhost:8083/connectors/hdfs-sink

Spark Job Submission
--------------------
Use the following commands to submit Spark jobs:

- For processing Avro files:
    spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-avro_2.12:3.5.2 /etc/spark/hdfs_processing_avro_out.py


HDFS Commands
-------------
- Retrieve a file from HDFS:
    hdfs dfs -get /kafka/topics/data-ingestion-topic/partition=0/data-ingestion-topic+0+0000000000+0000000000.avro

- Remove Avro files from HDFS:
    hdfs dfs -rm /kafka/topics/data-ingestion-topic/partition=0/*.avro

- Get the schema file from HDFS:
    hdfs dfs -get /user/hive/warehouse/schemafile.avsc

Schema Registry Management
-------------------------
- Register a new Avro schema:
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @/schemas/avro_schema.json http://localhost:8081/subjects/data-ingestion-topic-value/versions

- List registered schemas:
    curl -X GET http://localhost:8081/subjects

- Get the latest version of a schema:
    curl -X GET "http://schema-registry:8081/subjects/data-ingestion-topic-value/versions/latest" -H "Accept: application/json" > /tmp/schema.avsc

- Upload the schema to HDFS:
    hdfs dfs -put /tmp/schema.avsc /user/hive/schema.avsc

Hive and Trino Integration
--------------------------
Creating an External Hive Table
-------------------------------
To create an external Hive table for querying the data, run the following command in the Hive service:

    hive
    CREATE EXTERNAL TABLE crimeData
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION 'hdfs://namenode:9000/kafka/topics/output'
    TBLPROPERTIES (
        'avro.schema.url'='hdfs://namenode:9000/user/hive/warehouse/schemafile.avsc'
    );

This table points to the processed data stored in HDFS.

Querying Data with Trino
------------------------
To test the connection between Trino and Hive, use the following SQL query in the Trino CLI:

    trino
    SELECT * FROM hive.default.crimeData LIMIT 10;

This retrieves sample records from the Hive table.

Sentry Integration
------------------
- Create a new Sentry user:
    sentry createuser --email admin@example.com --password mypassword --superuser


Updating Sentry DSN
-------------------
Use the following command to update the Sentry DSN in the Spark container:
    echo "http://88b656adedca4b4b9f1f3b3e365a6be7@sentry-sentry-1:9000/1" | tee /etc/spark/sentry_dsn.txt > /dev/null

Troubleshooting
---------------
- Ensure that all containers are running by using the command:
    docker ps
- Check the logs of any service with:
    docker logs <container_name>
- If the web interfaces for Airflow or Sentry are not available, give them a few extra minutes to fully initialize.