"""
Create evolving_user_status_changed table
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['MY_SCHEMA']
TABLE_NAME = 'evolving_user_status_changed'


query_drop_tbl = """
    DROP TABLE IF EXISTS {0}.{1};
""".format(SCHEMA, TABLE_NAME)


query_create_tbl = """
    CREATE TABLE IF NOT EXISTS {0}.{1}(
        event_id integer IDENTITY(0,1),
        user_id integer NOT NULL,
        snapshot_datestart date NOT NULL,
        snapshot_dateend date NULL,
        status varchar(15) NOT NULL,
        PRIMARY KEY(event_id)
    );
""".format(SCHEMA, TABLE_NAME)


query_insert_tbl = """
    INSERT INTO {0}.{1}(user_id, snapshot_datestart, snapshot_dateend, status)
        WITH all_statuses AS
        (
            SELECT
              user_id,
              event_date,
              status
            FROM
              {0}.derived_user_activated
            UNION
            SELECT
              user_id,
              event_date,
              status
            FROM
              {0}.derived_user_churned
            UNION
            SELECT
              user_id,
              event_date,
              status
            FROM
              {0}.derived_user_became_inactive
        )
        SELECT
            user_id,
            event_date as snapshot_datestart,
            LEAD(event_date) OVER(PARTITION BY user_id ORDER BY event_date) as snapshot_dateend,
            status
        FROM
            all_statuses;
""".format(SCHEMA, TABLE_NAME)


def load_evolving_user_status_changed(conn):
    execute_queries(conn=conn, queries=[query_drop_tbl, query_create_tbl,
                                        query_insert_tbl])
    print('Upload complete: {}'.format(TABLE_NAME))
