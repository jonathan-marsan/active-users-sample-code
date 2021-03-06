"""
Create snapshot_user_churn_rate_monthly table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'snapshot_user_churn_rate_monthly'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        month date NOT NULL,
        user_retention_rate decimal(9,4) NULL,
        user_churn_rate decimal(9,4) NULL
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(month, user_retention_rate, user_churn_rate)
        WITH monthly_events as
        (
            SELECT
              lookup_months.snapshot_datestart,
              status as current_status,
              LAG(status) OVER(PARTITION BY user_id ORDER BY lookup_months.snapshot_datestart) as prev_status
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
        )
        SELECT
            monthly_events.snapshot_datestart as month,
            CAST((SUM(CASE WHEN prev_status = 'ACTIVE' THEN 1 ELSE 0 END) - SUM(CASE WHEN prev_status = 'ACTIVE' AND current_status IN('CHURNED', 'INACTIVE') THEN 1 ELSE 0 END))::float/NULLIF(SUM(CASE WHEN prev_status = 'ACTIVE' THEN 1 ELSE 0 END)::float, 0) as decimal(9,4)) as user_retention_rate,
            1 - CAST((SUM(CASE WHEN prev_status = 'ACTIVE' THEN 1 ELSE 0 END) - SUM(CASE WHEN prev_status = 'ACTIVE' AND current_status IN('CHURNED', 'INACTIVE') THEN 1 ELSE 0 END))::float/NULLIF(SUM(CASE WHEN prev_status = 'ACTIVE' THEN 1 ELSE 0 END)::float, 0) as decimal(9,4)) as user_churn_rate
        FROM
            monthly_events
        GROUP BY
            monthly_events.snapshot_datestart;
""".format(SCHEMA, TABLE_NAME)


def load_snapshot_user_churn_rate_monthly(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
