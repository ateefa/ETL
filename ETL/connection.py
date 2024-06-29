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

def insert(conn,df,table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()



# conn = connect(param_dic)
# csv_file = r"/home/ateefa/Project/Demo/Data/Input/AB_NYC_2019.csv"
# df = pd.read_csv(csv_file)
# df = df.drop(['last_review','availability_365','name','host_name','minimum_nights'], axis=1)
# df['reviews_per_month'].fillna(0, inplace=True)
# cur = conn.cursor()
# insert(conn, df,'data')
# n_rows = execute_query(conn, "select count(*) from data;")
#
# print("Number of rows in the table = %s" % n_rows)
# cur.close()
# conn.close()