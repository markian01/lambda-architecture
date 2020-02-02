#!/bin/bash
CQL="
  CREATE KEYSPACE IF NOT EXISTS reddit WITH replication = {
      'class': 'SimpleStrategy', 'replication_factor': '1'
  };
  USE reddit;
  CREATE TABLE windowed_submissions (date TIMESTAMP, window TIMESTAMP, count INT, PRIMARY KEY (date, window));
  CREATE TABLE windowed_comments (date TIMESTAMP, window TIMESTAMP, count INT, PRIMARY KEY (date, window));
  CREATE TABLE unique_active_subreddits (date TIMESTAMP, window TIMESTAMP, active_subreddits INT, submissions_and_comments INT, PRIMARY KEY (date, window));
  CREATE TABLE trending_subreddits (subreddit TEXT, submissions INT, comments INT, total INT, last_update TIMESTAMP, PRIMARY KEY (subreddit));
"

until echo $CQL | cqlsh; do
  echo "cassandra-init.sh: Cassandra is not yet initialized - will retry later.."
  sleep 10
done &

exec /entrypoint.sh dse cassandra -f