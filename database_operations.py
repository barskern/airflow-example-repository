"""
Implemets databases operations such as 
- creates database
- creates table
- inssert rows into a table
- drop a table
Uses local host by default.
"""

import psycopg2 as psg
from psycopg2.extras import execute_values
import pandas as pd
from sql_queries import create_table_queries
from sql_queries import drop_table_queries
from sql_queries import user_table_insert, artist_table_insert, song_table_insert

DATABASE_HOST = os.getenv('DATABASE_HOST', '127.0.0.1')
DATABASE_USER = os.getenv('DATABASE_USER', 'postgres')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', 'admin')

def connect_to_database(database_name):
    """Creates a connection.
    """
    conn = psg.connect(host=DATABASE_HOST, dbname=database_name, \
                 user=DATABASE_USER, password=DATABASE_PASSWORD)
    return conn


def create_database(database_name: str):
    """Creates a database

    Args:
        database_name: the target database name. 
        username: the username
        password: the password
    """
    conn = psg.connect(host=DATABASE_HOST, user=DATABASE_USER, password=DATABASE_PASSWORD)
    cursor = conn.cursor()
    conn.set_session(autocommit=True)
    cursor.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cursor.execute(f'CREATE DATABASE {database_name} WITH ENCODING "utf8" TEMPLATE template0')
    conn.close()


def create_table(conn):
    cursor = conn.cursor()
    for query in create_table_queries:
        cursor.execute(query)
        conn.commit()
    cursor.close()


def insert_dataframe(conn, df, table_name):
    df2 = df.where((pd.notnull(df)), None)

    with conn.cursor() as c:
        if table_name == "songs":
            sql = song_table_insert
        elif table_name == "artists":
            sql = artist_table_insert
        elif table_name == "users":
            sql = user_table_insert
        else:
            raise RuntimeError("Unsupported table {table_name}")

        execute_values(
                cur=c,
                sql=sql,
                argslist=[tuple(row) for row in df2.to_numpy()]
        )

    conn.commit()


def drop_tables(conn, cursor):
    cursor = conn.cursor()
    for query in drop_table_queries:
        cursor.execute(query)
        conn.commit()
    cursor.close()
