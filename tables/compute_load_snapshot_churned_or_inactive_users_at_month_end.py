"""
Create snapshot_churned_or_inactive_users_at_month_end table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'snapshot_churned_or_inactive_users_at_month_end'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        snapshot_datestart date NOT NULL,
        snapshot_dateend date NOT NULL,
        total_churned_or_inactive_users integer NOT NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(snapshot_datestart, snapshot_dateend, total_churned_or_inactive_users)
            SELECT
              lookup_months.snapshot_datestart,
              lookup_months.snapshot_dateend,
              COUNT(*) as total_churned_or_inactive_users
            FROM
              jmarsan.lookup_months
            LEFT JOIN
              jmarsan.evolving_user_status_changed
            ON
              evolving_user_status_changed.snapshot_datestart <= lookup_months.snapshot_dateend
              AND
              lookup_months.snapshot_dateend < evolving_user_status_changed.snapshot_dateend
            WHERE
              evolving_user_status_changed.status IN('CHURNED','INACTIVE')
            GROUP BY
              lookup_months.snapshot_datestart, lookup_months.snapshot_dateend;
""".format(SCHEMA, TABLE_NAME)


def load_snapshot_churned_or_inactive_users_at_month_end(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
