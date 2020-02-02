STRING = 'string'
INTEGER = 'integer'
DOUBLE = 'double'
BOOLEAN = 'boolean'
TIMESTAMP = 'timestamp'

try:
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType,
        DoubleType, BooleanType
    )

    SPARK_DTYPE_MAPPING = { STRING: StringType(),
                            INTEGER: IntegerType(),
                            DOUBLE: DoubleType(),
                            BOOLEAN: BooleanType() }
except (ModuleNotFoundError, NameError):
    pass

MODELS = { 'SubmissionProducer': { 
                'schema': { 'id': STRING,
                            'subreddit': STRING,
                            'subreddit_subscribers': INTEGER,
                            'title': STRING,
                            'author': STRING,
                            'created_utc': DOUBLE,
                            'over_18': BOOLEAN,
                            'selftext': STRING },
                'to_str': [ 'subreddit', 
                            'author' ]},
            'CommentProducer': { 
                'schema': { 'id': STRING,
                            'subreddit': STRING,
                            'link_id': STRING,
                            'author': STRING,
                            'created_utc': DOUBLE,
                            'body': STRING },
                'to_str': [ 'subreddit',
                            'author' ]},
            'SubredditProducer': {
                'schema': { 'id': STRING,
                            'display_name': STRING,
                            'description': STRING,
                            'created_utc': DOUBLE,
                            'over18': BOOLEAN,
                            'subscribers': INTEGER },
                'to_str': []},
            'RedditorProducer': {
                'schema': { 'id': STRING,
                            'name': STRING,
                            'created_utc': DOUBLE },
                'to_str': []}}

def generate_kafka_keys(model):
    model = MODELS[model]
    return model['schema'], model['to_str']

def generate_spark_schema(model):
    fields = list()
    schema = MODELS[model]['schema']
    for key, dtype in schema.items():
        field = StructField(key, SPARK_DTYPE_MAPPING[dtype], True)
        fields.append(field)
    return StructType(fields)
