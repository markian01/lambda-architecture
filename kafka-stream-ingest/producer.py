import os
import json
import logging
from pathlib import Path

from confluent_kafka import Producer
from praw import Reddit

from schema_generator import generate_kafka_keys

client_id = '__client_id__'
secret = '__secret__'
user_agent = '__user_agent__'

KAFKA_SOCKET = os.environ.get('KAFKA_SOCKET')

class RedditProducer():
    conf = {'bootstrap.servers': KAFKA_SOCKET}

    def __init__(self, topic):
        if type(self) == RedditProducer:
            raise Exception('<RedditProducer> must be subclassed.')
        
        self.producer = Producer(self.conf)
        self.reddit = Reddit(client_id=client_id, 
                             client_secret=secret, 
                             user_agent=user_agent)
        self.topic = topic
        self.logger = self._get_logger()
    
    @classmethod
    def get_producer_subclass(cls, topic):
        subclasses_map = { subclass.__name__.lower().replace('producer', ''): subclass
                           for subclass in cls.__subclasses__() }
        Subclass = subclasses_map[topic]
        return Subclass(Subclass.__name__)

    def produce(self):
        raise NotImplementedError

    def _produce(self, stream):
        keys, to_str = generate_kafka_keys(self.topic)
        for data in stream:
            try:
                value = self._create_dict_from_obj(data, keys, to_str)
                self.producer.produce(self.topic,
                                      self._serialize(value),
                                      callback=self._callback)
                self.producer.poll(0)
            # except ValueError:
            #     self.logger.error(f'Invalid input - {value}. Discarding record.')
            except (SystemExit, KeyboardInterrupt, GeneratorExit, Exception) as e:
                self.producer.flush()
                self.logger.error(f'{type(e).__name__}: {e}')
                raise

    def _get_logger(self):
        root = Path('/logs')
        logger = logging.getLogger(self.topic)
        logger.setLevel(logging.INFO)
        f_handler = logging.FileHandler(root.joinpath(f'{self.topic}.log'), mode='w')
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        f_handler.setFormatter(f_format)
        f_handler.setLevel(logging.INFO)
        logger.addHandler(f_handler)
        return logger

    def _callback(self, err, msg):
        if err is None:
            self.logger.info(f'Ingest succeeded: {msg.value()}')
        else:
            self.logger.error(f'Ingest failed: {msg.value()} {err.str()}')

    @staticmethod
    def _serialize(value):
        return json.dumps(value).encode()
    
    @staticmethod
    def _create_dict_from_obj(obj, keys, to_str):
        _getattr = lambda obj, key, to_str: getattr(obj, key) if key not in to_str else str(getattr(obj, key))
        return {key: _getattr(obj, key, to_str) for key in keys}

class SubmissionProducer(RedditProducer):
    def produce(self):
        stream = self.reddit.subreddit('all').stream.submissions()
        self._produce(stream)

class CommentProducer(RedditProducer):
    def produce(self):
        stream = self.reddit.subreddit('all').stream.comments()
        self._produce(stream)

class SubredditProducer(RedditProducer):
    def produce(self):
        stream = self.reddit.subreddits.stream()
        self._produce(stream)

class RedditorProducer(RedditProducer):
    def produce(self):
        stream = self.reddit.redditors.stream()
        self._produce(stream)
