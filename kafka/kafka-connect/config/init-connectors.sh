#!/bin/bash

# Start Kafka Connect in the background
/etc/confluent/docker/run &

# Log the start of the wait period
echo "Waiting for Kafka Connect to start listening on kafka-connect ⏳"

# Wait for Kafka Connect to be ready
while [ $(curl -s -o /dev/null -w %{http_code} http://kafka-connect:8083/connectors) -eq 000 ]; do 
  echo -e $(date) " Kafka Connect listener HTTP state: $(curl -s -o /dev/null -w %{http_code} http://kafka-connect:8083/connectors) (waiting for 200)"
  sleep 5 
done

# Verify Kafka Connect is listening on port 8083
nc -vz kafka-connect 8083

# Log the start of the connector creation
echo -e "\n--\n+> Creating Kafka Connect hdfs sink"

# Execute the script to create the Elasticsearch sink connector
curl -X POST -H "Content-Type: application/json" --data @/etc/kafka-connect/connect-hdfs-sink.json http://localhost:8083/connectors

echo -e "\n--\n+> Kafka Connector created"

# Keep the container running indefinitely
sleep infinity
