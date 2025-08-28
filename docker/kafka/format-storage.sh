#!/bin/bash

set -e

DATA_DIR=/var/lib/kafka/data
#DATA_DIR=/home/dataeng/github/streaming-batch-project-2/docker/kafka/data
CLUSTER_ID="qfufNFYwQCOkbk9liQvPIg"

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  echo "Formatting storage directory with CLUSTER_ID=$CLUSTER_ID"
  kafka-storage format --ignore-formatted --cluster-id "$CLUSTER_ID" --config /etc/kafka/kafka.properties
else
  echo "Storage directory already formatted."
fi

exec /etc/confluent/docker/run

