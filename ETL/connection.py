#------------------------------------------------------------------------------
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
param_dic = {"host": "localhost","database": "ETL","user": "postgres","password": "12345","port": 5432}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def execute_query(conn, query):
    """ Execute a single query """

    ret = 0  # Return value
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    # If this was a select query, return the result
    if 'select' in query.lower():
        ret = cursor.fetchall()
    cursor.close()
    return ret


def commit(connection):
    connection.commit()

def close(connection,cursor):
    connection.close()
    cursor.close()


conn = connect(param_dic)
n_rows = execute_query(conn, "select count(*) from test;")
print("Number of rows in the table = %s" % n_rows)
