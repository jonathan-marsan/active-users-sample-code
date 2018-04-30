"""
Create lookup_months table
"""

import os

from utilities.db_connection import db_connection, execute_queries


# CONSTANTS
SCHEMA = os.environ['SCHEMA_JM']
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
          snapshot_datestart,
          CAST(DATEADD(month, 1, snapshot_datestart) as date)
        FROM
          jmarsan.lookup_dates
        WHERE
          RIGHT(snapshot_datestart, 2) = '01';
""".format(SCHEMA, TABLE_NAME)


execute_queries(conn=db_connection(), queries=[query_drop_tbl,
                                               query_create_tbl,
                                               query_insert_tbl])
