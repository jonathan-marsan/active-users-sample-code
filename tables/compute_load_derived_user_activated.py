"""
Create derived_user_activated table
"""

import os

from utilities.db_connection import db_connection, execute_queries


ACTIVE_PERIOD = 28
SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'derived_user_activated'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE {0}.{1}(
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
          task_date as event_date,
          'ACTIVE' as status
        FROM
          {0}.user_tasks_with_active_period
        WHERE
          (prev_task_date IS NULL
          OR
          DATEDIFF(day, prev_task_date, task_date) > {2});
""".format(SCHEMA, TABLE_NAME, ACTIVE_PERIOD)


def load_derived_user_activated(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
