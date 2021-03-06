from pyspark.sql.functions import (
    window, lit, approx_count_distinct,
    col, count, to_date)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, TimestampType

from stream_processor import (
    spark_session, stream_from_kafka,
    write_to_hdfs, stream_to_cassandra)

def main():
    # base streams from kafka
    submission_stream = stream_from_kafka('SubmissionProducer')
    comment_stream = stream_from_kafka('CommentProducer')
    # subreddit_stream = stream_from_kafka('SubredditProducer')
    # redditor_stream = stream_from_kafka('RedditorProducer')

    # persist base streams to hdfs for batch processing
    write_to_hdfs(submission_stream, 'submission')
    write_to_hdfs(comment_stream, 'comment')
    # write_to_hdfs(subreddit_stream, 'subreddit')
    # write_to_hdfs(redditor_stream, 'redditor')

    # streaming stats
    windowed_submissions = submission_stream \
        .groupby(window('created_utc', '5 seconds', '5 seconds')) \
        .count() \
        .withColumn('window', col('window')['end']) \
        .withColumn('date', to_date('window'))
        
    windowed_comments = comment_stream \
        .groupby(window('created_utc', '5 seconds', '5 seconds')) \
        .count() \
        .withColumn('window', col('window')['end']) \
        .withColumn('date', to_date('window'))

    windowed_unique_active_subreddits = submission_stream \
        .select('created_utc', 'subreddit') \
        .union(comment_stream.select('created_utc', 'subreddit')) \
        .groupby(window('created_utc', '5 seconds', '5 seconds')) \
        .agg(approx_count_distinct('subreddit'), count('subreddit')) \
        .withColumn('window', col('window')['end']) \
        .withColumn('date', to_date('window')) \
        .withColumnRenamed('approx_count_distinct(subreddit)', 'active_subreddits') \
        .withColumnRenamed('count(subreddit)', 'submissions_and_comments')

    # persist streaming stats to cassandra
    stream_to_cassandra(windowed_submissions, 'windowed_submissions')
    stream_to_cassandra(windowed_comments, 'windowed_comments')
    stream_to_cassandra(windowed_unique_active_subreddits, 'unique_active_subreddits')

    spark_session.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
