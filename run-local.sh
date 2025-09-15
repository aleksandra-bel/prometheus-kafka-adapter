#!/bin/bash

#
# Script to run the prometheus-kafka-adapter locally for testing the InfluxDB polling feature.
#
# Before running this script, make sure you have:
# 1. Started the services with `docker-compose up -d`.
# 2. Configured InfluxDB and obtained your API token.
#
# Then, edit this file and replace `<your-influxdb-api-token>` with your actual token.
#

export INFLUXDB_URL="http://localhost:8086"
export INFLUXDB_TOKEN="<your-influxdb-api-token>"
export INFLUXDB_ORG="my-org"
export INFLUXDB_BUCKET="my-bucket"
export INFLUXDB_MEASUREMENTS="cpu_load,memory_usage" # Add the measurements you want to poll
export INFLUXDB_POLLING_INTERVAL="10s"
export INFLUXDB_KAFKA_TOPIC="influxdb-metrics"
export KAFKA_BROKER_LIST="localhost:9092"

# Set a log level (e.g., "info", "debug")
export LOG_LEVEL="info"

echo "Starting prometheus-kafka-adapter..."
echo "---"
echo "InfluxDB URL:             $INFLUXDB_URL"
echo "InfluxDB Org:             $INFLUXDB_ORG"
echo "InfluxDB Bucket:          $INFLUXDB_BUCKET"
echo "Measurements to Poll:     $INFLUXDB_MEASUREMENTS"
echo "Polling Interval:         $INFLUXDB_POLLING_INTERVAL"
echo "Kafka Topic:              $INFLUXDB_KAFKA_TOPIC"
echo "Kafka Brokers:            $KAFKA_BROKER_LIST"
echo "---"

go run .
