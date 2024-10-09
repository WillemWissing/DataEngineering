#!/bin/bash

# Check if SPARK_MODE is set
if [ -z "$SPARK_MODE" ]; then
  echo "SPARK_MODE is not set. Exiting..."
  exit 1
fi

case "$SPARK_MODE" in
  master)
    echo "Starting Spark master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
      --host $(hostname) \
      --port 7077 \
      --webui-port 8080
    ;;
  
  worker)
    if [ -z "$SPARK_MASTER_URL" ]; then
      echo "SPARK_MASTER_URL is not set. Exiting..."
      exit 1
    fi
    echo "Starting Spark worker..."
  
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
      $SPARK_MASTER_URL \
      --webui-port 8081
    ;;
  
  submit)
    if [ -z "$SPARK_MASTER_URL" ]; then
      echo "SPARK_MASTER_URL is not set. Exiting..."
      exit 1
    fi
    ;;
  
  *)
    echo "Invalid SPARK_MODE: $SPARK_MODE. Exiting..."
    exit 1
    ;;
esac

# Keep the container alive only in submit mode if needed
if [ "$SPARK_MODE" == "submit" ]; then
  tail -f /dev/null
fi
