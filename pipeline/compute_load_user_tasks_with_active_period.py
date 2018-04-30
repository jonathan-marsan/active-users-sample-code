"""
Create user_tasks_with_active_period view
"""

import os

from utilities.db_connection import db_connection, execute_query


ACTIVE_PERIOD = 28
SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'user_tasks_with_active_period'


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
            user_id < 10;
""".format(SCHEMA, TABLE_NAME, ACTIVE_PERIOD)


execute_query(conn=db_connection(), query=query_create_view)
