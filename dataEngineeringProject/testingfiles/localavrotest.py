"""
Avro File Reader and Deserializer

This script reads a binary Avro file, deserializes its content using a schema fetched
from a schema registry, and prints both the raw data and the deserialized records.
"""

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Schema registry configuration
schema_registry_conf = {'url': 'http://localhost:8087'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

def get_avro_schema(schema_registry_client, schema_id):
    """
    Fetch the schema definition from the schema registry using the given schema ID.
    
    Args:
        schema_registry_client: The Schema Registry client.
        schema_id (int): The ID of the schema to retrieve.
    
    Returns:
        Schema: The retrieved schema object.
    """
    schema = schema_registry_client.get_schema(schema_id)
    return schema

def create_avro_deserializer(schema_registry_client, schema_id):
    """
    Create an Avro deserializer using the schema from the schema registry.
    
    Args:
        schema_registry_client: The Schema Registry client.
        schema_id (int): The ID of the schema to use for deserialization.
    
    Returns:
        AvroDeserializer: The deserializer for Avro messages.
    """
    schema = get_avro_schema(schema_registry_client, schema_id)
    return AvroDeserializer(schema_registry_client, schema)

def read_and_deserialize_avro_file(file_path, schema_id):
    """
    Read an Avro file and deserialize its content using the specified schema ID.
    
    Args:
        file_path (str): The path to the Avro file.
        schema_id (int): The ID of the schema to use for deserialization.
    """
    # Create the Avro deserializer with the schema ID
    avro_deserializer = create_avro_deserializer(schema_registry_client, schema_id)

    with open(file_path, "rb") as f:
        # Read the entire content of the Avro file
        avro_content = f.read()

        # Print the raw Avro data (binary)
        print("Raw Avro data (unconverted):")
        print(avro_content)
        print(" ")

        # Try to deserialize the Avro content
        try:
            record = avro_deserializer(avro_content, SerializationContext('data-ingestion-topic', MessageField.VALUE))
            print("Deserialized message:")
            print(record)
        except Exception as e:
            print(f"Error deserializing message: {e}")

def main():
    """
    Main function to execute the script. Defines the path to the Avro file and the schema ID
    for deserialization.
    """
    avro_file_path = 'testfile.avro'  # Path to your local Avro file
    schema_id = 1  # Replace with the correct schema ID

    # Load and deserialize Avro file
    read_and_deserialize_avro_file(avro_file_path, schema_id)

if __name__ == "__main__":
    main()  # Entry point of the script
