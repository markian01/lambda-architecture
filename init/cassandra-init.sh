#!/bin/bash
CQL="
  CREATE KEYSPACE IF NOT EXISTS reddit WITH replication = {
      'class': 'SimpleStrategy', 'replication_factor': '1'
  };
  USE reddit;
  CREATE TABLE windowed_submissions (window TIMESTAMP, submissions_last_5_mins INT, PRIMARY KEY (window));
  CREATE TABLE windowed_comments (window TIMESTAMP, comments_last_5_mins INT, PRIMARY KEY (window));
  CREATE TABLE unique_active_subreddits (window TIMESTAMP, active_subreddits INT, PRIMARY KEY (window));
  CREATE TABLE trending_subreddits (subreddit TEXT, submissions INT, comments INT, total INT, last_update TIMESTAMP, PRIMARY KEY (subreddit));
"

until echo $CQL | cqlsh; do
  echo "cassandra-init.sh: Cassandra is not yet initialized - will retry later.."
  sleep 10
done &

exec /entrypoint.sh dse cassandra -f