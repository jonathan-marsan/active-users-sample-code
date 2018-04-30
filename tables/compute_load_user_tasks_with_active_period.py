"""
Create user_tasks_with_active_period view
"""

import os

from utilities.db_connection import execute_queries


ACTIVE_PERIOD = 28
SCHEMA = os.environ['SCHEMA_JM']
VIEW_NAME = 'user_tasks_with_active_period'


query_drop_view = """
    DROP VIEW IF EXISTS {0};
""".format(VIEW_NAME)


query_create_view = """
    CREATE VIEW {0}.{1} AS
        SELECT
            user_id,
            LAG(date, 1) OVER(PARTITION BY user_id ORDER BY date) as prev_task_date,
            date as task_date,
            LEAD(date, 1) OVER(PARTITION BY user_id ORDER BY date) as next_task_date,
            CAST(DATEADD(DAY, {2}, date) AS DATE) as task_date_period_end,
            CAST(DATEADD(DAY, {2}*2, date) AS DATE) as cancel_date_period_end
        FROM
            source_data.tasks_used_da
        WHERE
            sum_tasks_used > 0
            AND
            user_id < 1000;
""".format(SCHEMA, VIEW_NAME, ACTIVE_PERIOD)


def load_user_tasks_with_active_period(conn):
    execute_queries(conn=conn, queries=[query_drop_view, query_create_view])
    print('Created view: {}'.format(VIEW_NAME))
