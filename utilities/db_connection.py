"""
Redshift connection and execute functions
"""

import os

import psycopg2


DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']


def db_connection():
    """
    Connect to Redshift
    """
    conn = psycopg2.connect(dbname= DB_NAME, host=DB_HOST, port= DB_PORT,
                            user= DB_USER, password= DB_PASSWORD)
    return conn


def execute_query(conn, query):
    """
    Execute Redshift query
    """
    print('Task started')
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    finally:
        conn.close()
    print('Task complete')


def execute_queries(conn, queries):
    """
    Execute several Redshift queries
    """
    print('Task started')
    try:
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
            conn.commit()
    finally:
        conn.close()
    print('Task complete')
