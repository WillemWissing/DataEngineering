import os
import requests
import json
import sentry_sdk
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from py4j.java_gateway import java_import

def initialize_sentry():
    """

    """
    with open("/etc/spark/sentry_dsn.txt", "r") as file:
        dsn = file.read().strip()

    if not dsn:
        raise ValueError("SENTRY_DSN variable is not set or is empty.")
    
    # Initialize Sentry with the DSN from the environment variable
    sentry_sdk.init(
        dsn=dsn,
        debug=True,
        shutdown_timeout=10
    )
    print("Sentry successfully initialized with DSN:", dsn)


def get_avro_schema_from_registry(schema_registry_url, subject):
    """
    Fetch the latest Avro schema from the Confluent Schema Registry.

    :param schema_registry_url: URL of the Schema Registry.
    :param subject: The subject for which the schema should be fetched.
    :return: Avro schema as a JSON string.
    """
    response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
    response.raise_for_status()
    schema_json = response.json()
    return schema_json['schema']


def spark_schema_to_avro(schema):
    """
    Convert a Spark DataFrame schema to Avro schema format, with correct handling of default values for nullable fields.

    :param schema: The Spark DataFrame schema.
    :return: Avro schema as a JSON string.
    """
    def field_to_avro(field):
        avro_type = 'string'  # Default to string type for simplicity
        default_value = None  # Default value for nullable fields

        field_type = field.dataType.typeName()
        if field_type == 'integer':
            avro_type = 'int'
            default_value = 0 if not field.nullable else None
        elif field_type == 'long':
            avro_type = 'long'
            default_value = 0 if not field.nullable else None
        elif field_type == 'float':
            avro_type = 'float'
            default_value = 0.0 if not field.nullable else None
        elif field_type == 'double':
            avro_type = 'double'
            default_value = 0.0 if not field.nullable else None
        elif field_type == 'boolean':
            avro_type = 'boolean'
            default_value = False if not field.nullable else None
        elif field_type == 'date' or field_type == 'timestamp':
            avro_type = 'string'  # Avro uses string for dates and timestamps
            default_value = "" if not field.nullable else None

        # Define Avro type, including null if nullable
        avro_type_definition = ['null', avro_type] if field.nullable else avro_type

        # Set default to null explicitly for nullable fields
        avro_field = {
            'name': field.name,
            'type': avro_type_definition
        }

        if field.nullable:
            avro_field['default'] = None  # Default for nullable fields is explicitly null
        else:
            avro_field['default'] = default_value

        return avro_field

    avro_schema = {
        'type': 'record',
        'name': 'SparkDataFrameSchema',
        'fields': [field_to_avro(field) for field in schema.fields]
    }

    return json.dumps(avro_schema, indent=2)


def write_schema_to_hdfs(spark, schema_str, schema_path):
    """
    Write the Avro schema of a DataFrame to HDFS using Hadoop's FileSystem API.

    :param spark: The active Spark session.
    :param schema_str: The Avro schema as a string.
    :param schema_path: HDFS path where the schema file will be saved.
    """
    # Get the Hadoop FileSystem object from the Spark session
    hadoop_conf = spark._jsc.hadoopConfiguration()
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.Path(schema_path)
    
    # Open an output stream to HDFS
    output_stream = fs.create(path, True)
    output_stream.write(schema_str.encode("utf-8"))
    output_stream.close()


def move_files_to_processed_folder(spark, source_dir, target_dir):
    """
    Move files from the source directory to the target directory in HDFS.

    :param spark: The active Spark session.
    :param source_dir: The HDFS directory where files are currently stored.
    :param target_dir: The HDFS directory to move the processed files to.
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    source_path = spark._jvm.Path(source_dir)
    target_path = spark._jvm.Path(target_dir)
    
    # Create the target directory if it doesn't exist
    if not fs.exists(target_path):
        fs.mkdirs(target_path)
    
    # List all files in the source directory
    file_status_list = fs.listStatus(source_path)
    
    # Move each file from source to target
    for file_status in file_status_list:
        src_file_path = file_status.getPath()
        dest_file_path = spark._jvm.Path(target_path, src_file_path.getName())
        fs.rename(src_file_path, dest_file_path)
        print(f"Moving file {src_file_path} to {dest_file_path}")


def main():
    try:
        # Initialize Sentry with the environment variable DSN
        initialize_sentry()

        # Start Spark session
        spark = SparkSession.builder \
            .appName("HDFS Avro Processing") \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.2,io.confluent:kafka-schema-registry-client:7.6.1") \
            .getOrCreate()

        # Path to the input directory in HDFS where Avro files are stored
        input_path = "hdfs://namenode:9000/kafka/topics/data-ingestion-topic/partition=0/*.avro"

        # Path to the output directory in HDFS
        output_path = "hdfs://namenode:9000/kafka/topics/output"
        
        # Path to the Avro schema file in HDFS
        schema_file_path = "hdfs://namenode:9000/user/hive/warehouse/schemafile.avsc"
        
        # Path to the processed files directory
        processed_files_path = "hdfs://namenode:9000/kafka/topics/processed"

        # Read the Avro files into a DataFrame
        df = spark.read.format("avro").load(input_path)

        # Perform some transformations: filter records where an arrest was made and select specific columns
        processed_df = df.filter(col("Arrest") == True).select(
            "ID", "CaseNumber", "Date", "PrimaryType", "Description", "Arrest", "Year", "Latitude", "Longitude"
        )

        # Check if the schema file exists in HDFS
        hadoop_conf = spark._jsc.hadoopConfiguration()
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.Path(schema_file_path)
        
        if not fs.exists(path):
            print(f"Schema file does not exist. Creating {schema_file_path}...")
            # Convert the schema to Avro format and write to HDFS
            avro_schema = spark_schema_to_avro(processed_df.schema)  # Convert Spark schema to Avro format
            write_schema_to_hdfs(spark, avro_schema, schema_file_path)

        # Log the schema and a few rows for inspection
        processed_df.printSchema()
        # processed_df.show(5, truncate=False)
        print(processed_df.schema)  # Check schema
        print(processed_df.count())  # Check number of rows

        # Write the processed DataFrame back to HDFS in Avro format
        processed_df.write.format("avro").mode("overwrite").save(output_path)

        # Move the processed Avro files to the "processed" directory
        move_files_to_processed_folder(spark, "hdfs://namenode:9000/kafka/topics/data-ingestion-topic/partition=0", processed_files_path)

    except Exception as e:
        # Capture any exceptions with Sentry
        sentry_sdk.capture_exception(e)
        print(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        if 'spark' in locals():
            spark.stop()
        sentry_sdk.flush()

if __name__ == "__main__":
    main()
