# Use the official Schema Registry image as the base image
FROM confluentinc/cp-schema-registry:7.2.0

USER root
RUN mkdir -p /schemas
USER appuser
# Copy the script and schema JSON file into the container
COPY ./schemas /schemas/

ENTRYPOINT ["/schemas/init-schema.sh"]


