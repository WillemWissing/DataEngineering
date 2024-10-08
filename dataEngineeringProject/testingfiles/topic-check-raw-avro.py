"""
Kafka Consumer for Avro Messages

This script consumes messages from a Kafka topic, deserializes them using Avro format,
and prints the raw and deserialized messages to the console.
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Schema registry configuration
schema_registry_conf = {'url': 'http://localhost:8087'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Create an Avro deserializer
avro_deserializer = AvroDeserializer(schema_registry_client)

# Consumer configuration for Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9093',  # Kafka broker address
    'group.id': 'data-ingestion-group',       # Consumer group ID
    'auto.offset.reset': 'earliest'           # Start consuming from the earliest message
}

# Initialize the Kafka consumer
consumer = Consumer(consumer_conf)
consumer.subscribe(['data-ingestion-topic'])  # Subscribe to the specified topic

try:
    while True:
        # Poll for new messages from Kafka
        msg = consumer.poll(1.0)
        
        if msg is None:
            print("No new message. Waiting for message...")
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Display the raw message (unconverted)
        raw_message = msg.value()
        print("\nRaw message (unconverted):")
        print(raw_message)

        # Try to deserialize the Avro message
        try:
            record = avro_deserializer(raw_message, SerializationContext('data-ingestion-topic', MessageField.VALUE))
            print("Deserialized message:")
            print(record)
        except Exception as e:
            print(f"Error deserializing message: {e}")

        # Exit after processing one message (for demonstration purposes)
        break

except KeyboardInterrupt:
    print("Aborted by user")
finally:
    consumer.close()  # Close the consumer to free resources
