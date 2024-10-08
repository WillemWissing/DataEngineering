"""
Schema Registry Fetcher and Validator

This script retrieves the latest Avro schema from a schema registry and validates its structure
to ensure it meets the expected format.
"""

import requests
import json

def get_schema_from_registry(schema_registry_url, schema_subject):
    """
    Fetch the schema from the schema registry using the specified subject.
    
    Args:
        schema_registry_url (str): The URL of the schema registry.
        schema_subject (str): The subject for which to fetch the schema.
    
    Returns:
        str or None: The schema in JSON format if successful, None otherwise.
    """
    url = f"{schema_registry_url}/subjects/{schema_subject}/versions/latest"
    response = requests.get(url)

    if response.status_code == 200:
        schema = response.json()['schema']
        print("Schema retrieved successfully:")
        print(schema)
        return schema
    else:
        print(f"Failed to retrieve schema: {response.status_code}")
        print(response.text)
        return None

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

# Configuration
schema_registry_url = "http://schema-registry:8087"  # URL of the Schema Registry
subject = "data-ingestion-topic-value"  # Subject for the schema

# Retrieve and validate schema
schema_json = get_schema_from_registry(schema_registry_url, subject)
if schema_json:
    validate_schema(schema_json)
