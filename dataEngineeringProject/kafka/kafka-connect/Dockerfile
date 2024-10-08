# Use the Apache Kafka Connect base image
FROM confluentinc/cp-kafka-connect:7.2.0

# Copy custom configurations
COPY ./config/. /etc/kafka-connect/

USER root

# Ensure the data directory exists with appropriate permissions
RUN mkdir -p /var/lib/kafka-connect/data && chmod -R 755 /var/lib/kafka-connect/data

ENTRYPOINT ["/etc/kafka-connect/init-connectors.sh"]

USER appuser


