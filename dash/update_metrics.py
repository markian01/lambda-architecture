import os
from datetime import timedelta

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST')
CASSANDRA_PORT = os.environ.get('CASSANDRA_PORT')

cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
session = cluster.connect('reddit')

def get_updates(allowance, **kwargs):
    table = kwargs['table']
    max_ts = kwargs['max_ts']
    if not max_ts:
        query = f'SELECT * FROM {table}'
    else:
        adjusted_max_ts = max_ts - timedelta(seconds=allowance * 5)
        query = f'SELECT * FROM {table} WHERE window > \'{adjusted_max_ts}\' ALLOW FILTERING'
    result = session.execute(query)
    rows = result.current_rows
    while result.has_more_pages:
        result = result.next()
        rows.extend(result.current_rows)
    return rows
