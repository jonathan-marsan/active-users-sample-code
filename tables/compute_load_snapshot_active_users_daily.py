"""
Create snapshot_active_users_daily table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'snapshot_active_users_daily'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        snapshot_date date NOT NULL,
        total_active_users integer NOT NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(snapshot_date, total_active_users)
            SELECT
              lookup_dates.snapshot_date,
              COUNT(*) as total_active_users
            FROM
              {0}.lookup_dates
            LEFT JOIN
              {0}.evolving_user_status_changed
            ON
              (evolving_user_status_changed.snapshot_datestart <= lookup_dates.snapshot_date
              AND
              lookup_dates.snapshot_date < evolving_user_status_changed.snapshot_dateend)
              OR
              (evolving_user_status_changed.snapshot_datestart <= lookup_dates.snapshot_date
              AND
              evolving_user_status_changed.snapshot_dateend IS NULL)
            WHERE
              evolving_user_status_changed.status = 'ACTIVE'
            GROUP BY
              lookup_dates.snapshot_date;
""".format(SCHEMA, TABLE_NAME)


def load_snapshot_active_users_daily(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
