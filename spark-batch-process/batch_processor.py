import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from hdfs import InsecureClient

from schema_generator import generate_spark_schema

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST')
NAMENODE_SOCKET = os.environ.get('NAMENODE_SOCKET')
HDFS_DATA = os.environ.get('HDFS_DATA')
HDFS_CHECKPOINT = os.environ.get('HDFS_CHECKPOINT')
HDFS_WRITE_OFFSET = os.environ.get('HDFS_WRITE_OFFSET')

spark_session = SparkSession.builder \
    .appName('batch-process') \
    .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
    .config('spark.cassandra.connection.keep_alive_ms', 30000) \
    .getOrCreate()
sc = spark_session.sparkContext

hdfs = InsecureClient(f'http://{NAMENODE_SOCKET}')

def get_updates():
    def get_write_offsets():
        try:
            write_offsets = spark_session.read.parquet(HDFS_WRITE_OFFSET + f'/offset') \
                            .groupby('source').max('offset_batch', 'offset')
            write_offsets = { offset.source: (offset.offset_batch, offset.offset)
                              for offset in write_offsets.collect() }
            return write_offsets
        except AnalysisException:
            return {}

    def get_parquets(source, max_offset_batch):
        batches = hdfs.list(f'/data/{source}')
        batches = filter(lambda r: r.startswith('_offset') and int(r.split('=')[1]) >= max_offset_batch, batches)
        parquets = [HDFS_DATA + f'/{source}/{batch}' for batch in batches]
        return parquets

    updates = dict()
    write_offsets = get_write_offsets()
    sources = hdfs.list('/data')
    for source in sources:
        max_offset_batch, max_offset = write_offsets.get(source, (0, 0))
        parquets = get_parquets(source, max_offset_batch)
        if not parquets:
            continue
        update = spark_session.read.parquet(*parquets).where(f'offset > {max_offset}')
        if not update.rdd.isEmpty():
            updates.update({source: update})
    return updates

def write_to_cassandra(df, table):
    df.write \
        .format('org.apache.spark.sql.cassandra') \
        .option('keyspace', 'reddit') \
        .option('table', table) \
        .mode('append') \
        .save()

def save_current_offsets(updates):
    current_offsets = [ (source, *update.groupby().max('offset_batch', 'offset').collect()[0])
                        for source, update in updates.items()]
    current_offsets = spark_session.createDataFrame(current_offsets, ['source', 'offset_batch', 'offset'])
    current_offsets.write \
        .format('parquet') \
        .option('path', HDFS_WRITE_OFFSET + '/offset') \
        .option('checkpointLocation', HDFS_CHECKPOINT + '/offset') \
        .mode('append') \
        .save()

def create_empty_df(table):
    topic = table.title() + 'Producer'
    schema = generate_spark_schema(topic)
    return spark_session.createDataFrame(sc.emptyRDD(), schema) 
