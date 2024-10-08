"""
Kafka Producer with Avro Serialization

This script reads data from a CSV file, converts each row into the Avro format based on a schema retrieved from a Schema Registry,
and produces the records to a specified Kafka topic in batches.
"""

import csv
import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from tqdm import tqdm

# Configuration
csv_file_path = 'data_subset.csv'  # Path to the input CSV file
kafka_broker = 'localhost:9093'  # Kafka broker address
kafka_topic = 'data-ingestion-topic'  # Kafka topic to produce messages to
schema_registry_url = 'http://localhost:8087'  # Schema Registry URL
schema_registry_subject = f"{kafka_topic}-value"  # Schema subject based on the topic name
batch_size = 10000  # Number of messages to produce in each batch

# Function to deliver messages
def delivery_report(err, msg):
    """
    Callback function to report message delivery status.

    Args:
        err: Error information if the delivery fails.
        msg: The delivered message.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    progress_bar.update(1)  # Update the progress bar for each delivered message

# Load the Avro schema from Schema Registry
def load_schema_from_registry(schema_registry_client, subject):
    """
    Load the latest Avro schema from the Schema Registry.

    Args:
        schema_registry_client: Instance of SchemaRegistryClient.
        subject: Subject name to fetch the schema.

    Returns:
        str: Avro schema as a string.
    """
    latest_version = schema_registry_client.get_latest_version(subject)
    return latest_version.schema.schema_str

# Function to convert a CSV value to the appropriate type based on Avro schema field type
def convert_value_to_avro_type(value, field_type):
    """
    Convert CSV values to their corresponding Avro types.

    Args:
        value: The CSV value as a string.
        field_type: The target Avro field type.

    Returns:
        The converted value or None if conversion fails.
    """
    if value == "":
        return None  # Return None for empty strings

    try:
        if field_type == "string":
            return value
        elif field_type == "int":
            return int(value)
        elif field_type == "float":  # Adjust according to the schema
            return float(value)
        elif field_type == "boolean":
            return value.lower() == "true"
        elif field_type == "long":
            return int(value)  # Use long in Avro for large integers
        elif field_type == "double":
            return float(value)
        elif field_type == "bytes":
            return bytes(value, encoding="utf-8")
        else:
            return value  # Default to string if the type is unrecognized
    except ValueError:
        return None  # Return None if conversion fails

# Function to convert a CSV row to match the Avro schema
def convert_row_to_avro_schema(row, schema_fields):
    """
    Convert a CSV row to an Avro-compatible format based on the schema fields.

    Args:
        row: The CSV row as a dictionary.
        schema_fields: List of fields defined in the Avro schema.

    Returns:
        dict: The converted row in Avro format.
    """
    avro_row = {}
    for field in schema_fields:
        field_name = field['name']
        field_type = field['type']
        
        if isinstance(field_type, dict):  # Handle complex types
            avro_row[field_name] = convert_value_to_avro_type(row[field_name], field_type['type'])
        elif isinstance(field_type, list):  # Handle unions, e.g., ["null", "int"]
            non_null_type = next(ft for ft in field_type if ft != "null")
            avro_row[field_name] = convert_value_to_avro_type(row[field_name], non_null_type)
        else:  # Handle primitive types
            avro_row[field_name] = convert_value_to_avro_type(row[field_name], field_type)
    return avro_row

# Schema Registry client
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
schema_str = load_schema_from_registry(schema_registry_client, schema_registry_subject)
schema_dict = json.loads(schema_str)  # Convert schema string to dictionary

# Extract fields from schema
schema_fields = schema_dict['fields']

# Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str=schema_str)

# Kafka Producer configuration
conf = {
    'bootstrap.servers': kafka_broker,
    'key.serializer': StringSerializer('utf_8'),  # Using string serializer for keys
    'value.serializer': avro_serializer,  # Avro serializer for values
    'batch.size': batch_size * 1024,  # Adjust Kafka batch size (bytes)
    'queue.buffering.max.messages': 1000000,  # Increase buffer size
    'queue.buffering.max.kbytes': 1048576  # Increase memory limit to 1GB
}

producer = SerializingProducer(conf)

# Read the total number of lines in the CSV file for the progress bar
with open(csv_file_path, 'r') as csvfile:
    total_messages = sum(1 for line in csvfile) - 1  # Subtract 1 for the header line

# Progress bar setup using tqdm
progress_bar = tqdm(total=total_messages, desc="Producing messages", unit="msg")

# Function to produce messages in batches
def produce_batch(rows):
    """
    Produce a batch of messages to Kafka.

    Args:
        rows: List of CSV rows to produce.
    """
    for row in rows:
        try:
            # Convert row to match the Avro schema types dynamically
            avro_row = convert_row_to_avro_schema(row, schema_fields)
            # Produce to Kafka
            producer.produce(
                topic=kafka_topic,
                key=str(avro_row['ID']),  # Assuming 'ID' is used as the key
                value=avro_row,
                on_delivery=delivery_report  # Delivery report callback
            )
        except Exception as e:
            print(f"Error producing message: {e}")

    # Poll to ensure delivery reports are handled
    producer.poll(0)

# Read and produce CSV data in batches
with open(csv_file_path, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    batch = []
    
    for row in reader:
        batch.append(row)  # Add row to the batch
        
        # Produce the batch if it reaches the specified size
        if len(batch) >= batch_size:
            produce_batch(batch)
            batch.clear()  # Clear the batch after sending
    
    # Produce any remaining rows in the last batch
    if batch:
        produce_batch(batch)

# Wait for all messages to be delivered
producer.flush()
progress_bar.close()
print('Job done')
