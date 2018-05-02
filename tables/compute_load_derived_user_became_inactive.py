"""
Create derived_user_became_inactive table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'derived_user_became_inactive'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


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
          {0}.user_tasks_with_active_period
        WHERE
          (
             next_task_date > task_date_period_end
             OR
             next_task_date IS NULL
          )
          AND
          (
             next_task_date > cancel_date_period_end
             OR
             next_task_date IS NULL
          )
          AND
          cancel_date_period_end < CURRENT_DATE;
""".format(SCHEMA, TABLE_NAME)


def load_derived_user_became_inactive(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
