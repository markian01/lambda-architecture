import os
import json
from math import floor

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import (
    udf, col, from_unixtime
)

from schema_generator import generate_spark_schema

KAFKA_SOCKET = os.environ.get('KAFKA_SOCKET')
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST')
HDFS_DATA = os.environ.get('HDFS_DATA')
HDFS_CHECKPOINT = os.environ.get('HDFS_CHECKPOINT')

spark_session = SparkSession.builder \
    .appName('stream-process') \
    .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
    .config('spark.cassandra.connection.keep_alive_ms', 30000) \
    .getOrCreate()

def stream_from_kafka(topic, decimals=-3):
    schema = generate_spark_schema(topic)
    deserialize = udf(lambda v: json.loads(v), schema)

    _get_offset_batch = lambda n, multiplier=10 ** decimals: int(floor(n * multiplier) / multiplier)
    get_offset_batch = udf(_get_offset_batch, IntegerType())

    return spark_session.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_SOCKET) \
        .option('subscribe', topic) \
        .load() \
        .withColumn('value', deserialize('value')) \
        .withColumn('_offset_batch', get_offset_batch('offset')) \
        .withColumn('offset_batch', col('_offset_batch')) \
        .select('*', 'value.*') \
        .withColumn('created_utc', from_unixtime('created_utc'))

def stream_to_cassandra(stream, table):
    def write_to_cassandra(batch, _, table):
        batch.write \
            .format('org.apache.spark.sql.cassandra')\
            .option('keyspace', 'reddit') \
            .option('table', table) \
            .mode('append') \
            .save()

    stream.writeStream \
        .outputMode('update') \
        .foreachBatch(lambda batch, _, table=table: write_to_cassandra(batch, _, table)) \
        .start()

def write_to_hdfs(stream, fname):
    stream.writeStream \
        .format('parquet') \
        .outputMode('append') \
        .option('path', HDFS_DATA + f'/{fname}') \
        .option('checkpointLocation', HDFS_CHECKPOINT + f'/{fname}') \
        .partitionBy('_offset_batch') \
        .start()

def stream_to_console(stream):
    stream.writeStream \
        .format('console') \
        .outputMode('update') \
        .start()
