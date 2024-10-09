#!/bin/bash

# Start the Schema Registry in the background
/etc/confluent/docker/run &

# Log the start of the wait period
echo "Waiting for Schema Registry to start listening on schema-registry:8081 ⏳"

# Wait for Schema Registry to be ready
while [ $(curl -s -o /dev/null -w %{http_code} http://schema-registry:8081/subjects) -ne 200 ]; do 
  echo -e $(date) " Schema Registry HTTP state: $(curl -s -o /dev/null -w %{http_code} http://schema-registry:8081/subjects) (waiting for 200)"
  sleep 5
done

# Verify Schema Registry is listening on port 8081
nc -vz schema-registry 8081

# Log the start of the schema creation
echo -e "\n--\n+> Adding schema to Schema Registry"

# Execute the script to post the schema to Schema Registry
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data @/schemas/avro_schema.json \
     http://schema-registry:8081/subjects/data-ingestion-topic-value/versions

# Keep the container running indefinitely
sleep infinity

