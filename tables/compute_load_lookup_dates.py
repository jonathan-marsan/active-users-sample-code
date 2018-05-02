"""
Create lookup_dates table
Reference: http://elliot.land/post/building-a-date-dimension-table-in-redshift
"""

import os

from utilities.db_connection import db_connection, execute_queries


# CONSTANTS
SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'lookup_dates'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE {0}.{1}(
        snapshot_date date NOT NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(snapshot_date)
        SELECT
          '2017-01-01' :: DATE + number AS snapshot_date
        FROM
          {0}.number
        WHERE
          number < 365
        ORDER BY
          number;
""".format(SCHEMA, TABLE_NAME)



def load_lookup_dates(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
