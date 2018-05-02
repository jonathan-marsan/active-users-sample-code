"""
Create lookup_months table
"""

import os

from utilities.db_connection import db_connection, execute_queries


# CONSTANTS
SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'lookup_months'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE {0}.{1}(
        snapshot_datestart date NOT NULL,
        snapshot_dateend date NOT NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(snapshot_datestart, snapshot_dateend)
        SELECT
          snapshot_date as snapshot_datestart,
          CAST(DATEADD(month, 1, snapshot_datestart) - interval '1 day' as date) as snapshot_dateend
        FROM
          jmarsan.lookup_dates
        WHERE
          RIGHT(snapshot_datestart, 2) = '01';
""".format(SCHEMA, TABLE_NAME)


def load_lookup_months(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
