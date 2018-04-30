"""
Create number table
Reference: http://elliot.land/post/redshift-does-not-support-generate-series
"""

import os

from utilities.db_connection import db_connection, execute_queries


SCHEMA = os.environ['SCHEMA_JM']
TABLE_NAME = 'number'

query_A = """
    CREATE TABLE IF NOT EXISTS {0}.{1} (
      {1} INTEGER NOT NULL
    ) DISTSTYLE ALL SORTKEY ({1});
    """.format(SCHEMA, TABLE_NAME)
query_B = "INSERT INTO {0}.{1} VALUES (1), (2), (3), (4), (5), (6), (7), (8);".format(SCHEMA, TABLE_NAME)
query_C = "INSERT INTO {0}.{1} SELECT {1} + 8 FROM {1};".format(SCHEMA, TABLE_NAME)
query_D = "INSERT INTO {0}.{1} SELECT {1} + 16 FROM {1};".format(SCHEMA, TABLE_NAME)
query_E = "INSERT INTO {0}.{1} SELECT {1} + 32 FROM {1};".format(SCHEMA, TABLE_NAME)
query_F = "INSERT INTO {0}.{1} SELECT {1} + 64 FROM {1};".format(SCHEMA, TABLE_NAME)
query_G = "INSERT INTO {0}.{1} SELECT {1} + 128 FROM {1};".format(SCHEMA, TABLE_NAME)
query_H = "INSERT INTO {0}.{1} SELECT {1} + 256 FROM {1};".format(SCHEMA, TABLE_NAME)
query_I = "INSERT INTO {0}.{1} VALUES (0);".format(SCHEMA, TABLE_NAME)


execute_queries(conn=db_connection(),
                queries=[query_A, query_B, query_C, query_D, query_E, query_F,
                         query_G, query_H, query_I])
