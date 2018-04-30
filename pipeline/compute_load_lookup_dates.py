"""
Create lookup_dates table
Reference: http://elliot.land/post/building-a-date-dimension-table-in-redshift
"""

import os

from utilities.db_connection import db_connection, execute_queries


# CONSTANTS
SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'lookup_dates'


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
          '2017-01-01' :: DATE + number AS snapshot_datestart,
          '2017-01-02' :: DATE + number AS snapshot_dateend
        FROM
          jmarsan.number
        WHERE
          number < 365
        ORDER BY
          number;
""".format(SCHEMA, TABLE_NAME)


execute_queries(conn=db_connection(), queries=[query_drop_tbl,
                                               query_create_tbl,
                                               query_insert_tbl])
