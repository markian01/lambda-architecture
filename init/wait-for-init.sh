#!/bin/bash

KAFKA_CHECK="docker exec -i zookeeper zookeeper-shell localhost:2181 ls /brokers/ids | tail -n 1 | grep -o [0-9]*"
CASSANDRA_CHECK="docker exec -i cassandra cqlsh -e 'DESCRIBE keyspaces' | grep reddit"

until eval $KAFKA_CHECK && eval $CASSANDRA_CHECK; do
  echo "wait-for-init.sh: Dependencies not yet ready - will retry later.."
  sleep 10
done