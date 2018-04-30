"""
Create derived_user_activated table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'derived_user_activated'


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
          jmarsan.user_tasks_with_active_period
        WHERE
          (prev_task_date IS NULL
          OR
          DATEDIFF(day, prev_task_date, task_date) > 28);
""".format(SCHEMA, TABLE_NAME)


execute_queries(conn=db_connection(),
                queries=[query_create_tbl,
                         query_insert_tbl])
