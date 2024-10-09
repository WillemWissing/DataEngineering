"""
Schema Registry Schema Fetcher

This script connects to a schema registry to fetch the latest schema for a given subject
and validates its structure to ensure it conforms to the Avro format.
"""

import json
from confluent_kafka.schema_registry import SchemaRegistryClient

# Configuration for Schema Registry
schema_registry_url = 'http://localhost:8087'
schema_registry_subject = 'data-ingestion-topic-value'  # Replace with your schema subject

# Initialize Schema Registry Client
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

# Fetch the latest schema version for the given subject
latest_version = schema_registry_client.get_latest_version(schema_registry_subject)

# Retrieve the schema definition
schema = latest_version.schema

# Convert schema to JSON format
schema_json = json.loads(schema.schema_str)
schemajson = json.dumps(schema_json, indent=4)

def validate_schema(schema_json):
    """
    Validate the retrieved schema to ensure it is valid JSON and has the correct Avro format.
    
    Args:
        schema_json (str): The schema in JSON format.
    """
    try:
        schema = json.loads(schema_json)
        print("Schema is valid JSON.")
        
        # Check if the schema has the required structure
        if schema.get('type') == 'record' and 'fields' in schema:
            print("Schema structure seems correct.")
        else:
            print("Schema structure is incorrect.")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")

# Validate the schema
validate_schema(schemajson)

# Print the schema in JSON format
print("Schema JSON:")
print(schemajson)
