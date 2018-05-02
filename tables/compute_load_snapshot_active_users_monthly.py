"""
Create snapshot_active_users_monthly table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'snapshot_active_users_monthly'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        month date NOT NULL,
        total_active_users integer NOT NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(month, total_active_users)
            SELECT
              lookup_months.snapshot_datestart as month,
              COUNT(*) as total_active_users
            FROM
              {0}.lookup_months
            LEFT JOIN
              {0}.evolving_user_status_changed
            ON
              (evolving_user_status_changed.snapshot_datestart <= lookup_months.snapshot_datestart
              AND
              lookup_months.snapshot_datestart < evolving_user_status_changed.snapshot_dateend)
              OR
              (evolving_user_status_changed.snapshot_datestart <= lookup_months.snapshot_datestart
              AND
              evolving_user_status_changed.snapshot_dateend IS NULL)
            WHERE
              evolving_user_status_changed.status = 'ACTIVE'
            GROUP BY
              lookup_months.snapshot_datestart;
""".format(SCHEMA, TABLE_NAME)


def load_snapshot_active_users_monthly(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
