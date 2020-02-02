import argparse

from producer import RedditProducer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reddit Kafka Producer')
    parser.add_argument('topic', type=str, help='Reddit Topics: [submission, comment, subreddit, redditor]')
    args = parser.parse_args()

    producer = RedditProducer.get_producer_subclass(args.topic)
    producer.produce()
