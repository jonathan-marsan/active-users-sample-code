"""
Create derived_user_became_inactive table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'derived_user_became_inactive'


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        event_id integer IDENTITY(0,1),
        user_id integer NOT NULL,
        event_date date NOT NULL,
        status varchar(15) NOT NULL,
        PRIMARY KEY(event_id)
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(user_id, event_date, status)
        SELECT
          user_id,
          cancel_date_period_end as event_date,
          'INACTIVE' as status
        FROM
          jmarsan.user_tasks_with_active_period
        WHERE
          (
             task_date_period_end < next_task_date
             OR
             next_task_date IS NULL
          )
          AND
          (
            next_task_date IS NULL
            OR
            next_task_date > cancel_date_period_end
          )
          AND
          DATEDIFF(day, cancel_date_period_end, CURRENT_DATE) > 28;
""".format(SCHEMA, TABLE_NAME)


execute_queries(conn=db_connection(),
                queries=[query_create_tbl,
                         query_insert_tbl])
