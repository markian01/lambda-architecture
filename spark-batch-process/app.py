import sys

from pyspark.sql.functions import lit, col
from pyspark.sql.types import TimestampType

from batch_processor import (
    get_updates, create_empty_df, 
    write_to_cassandra, save_current_offsets)

def main():
    updates = get_updates()
    if not updates:
        sys.exit()

    submission = updates.get('submission', create_empty_df('submission'))
    comment = updates.get('comment', create_empty_df('comment'))
    # subreddit = updates.get('subreddit', create_empty_df('subreddit'))
    # redditor = updates.get('redditor', create_empty_df('redditor'))

    last_update = submission.union(comment).agg({'created_utc': 'max'}).collect()[0][0]
    if last_update:
        trending_subreddits = submission \
            .select('subreddit') \
            .withColumn('kind', lit('submissions')) \
            .union(comment.select('subreddit')
                .withColumn('kind', lit('comments'))) \
            .groupby('subreddit') \
            .pivot('kind') \
            .count() \
            .fillna(0) \
            .withColumn('total', col('submissions') + col('comments')) \
            .orderBy(col('total').desc()) \
            .limit(25) \
            .withColumn('last_update', lit(last_update).cast(TimestampType()))

        write_to_cassandra(trending_subreddits, 'trending_subreddits')

    save_current_offsets(updates)


if __name__ == '__main__':
    main()
