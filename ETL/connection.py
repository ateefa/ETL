#------------------------------------------------------------------------------
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import seaborn as sns
def connect_with_db():
    param_dic = {"host": "localhost", "database": "ETL", "user": "postgres", "password": "12345", "port": 5432}
    return connect(param_dic)
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

def load_from_db(conn):
    cur = conn.cursor()
    cur.execute("select * from data")
    df = pd.DataFrame(cur.fetchall(), columns=['id', 'host_id', 'neighbourhood_group', 'neighbourhood', 'latitude',
                                               'longitude', 'room_type', 'price', 'number_of_reviews',
                                               'reviews_per_month', 'calculated_host_listings_count'])
    return df


conn = connect_with_db()
df = load_from_db(conn)



# avg_price = df.groupby('neighbourhood')['price'].agg(np.mean)
# print(avg_price)
# df['last_updated'] = dt.datetime.today().strftime("%m/%d/%Y")
# df['Year'] = pd.to_datetime(df['last_updated']).dt.year
# latest_yr_count = df.where(df['Year'] == 2024)
# bnb_booked = latest_yr_count['neighbourhood_group'].value_counts()
# bnb_booked = bnb_booked.rename_axis('unique_values').reset_index(name='counts')
# print(bnb_booked)

# print(df)

# csv_file = r"/home/ateefa/Project/Demo/Data/Input/AB_NYC_2019.csv"
# df = pd.read_csv(csv_file)
# df = df.drop(['last_review','availability_365','name','host_name','minimum_nights'], axis=1)
# df['reviews_per_month'].fillna(0, inplace=True)
# insert(conn, df,'data')

# n_rows = execute_query(conn, "select count(*) from data;")
#
# print("Number of rows in the table = %s" % n_rows)
# cur.close()
# conn.close()

