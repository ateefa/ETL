import pandas as pd

df = pd.read_csv(r"/home/ateefa/Project/Demo/Data/Input/AB_NYC_2019.csv")

df.head()
df.describe()
print(df)


# from extract import extract_data
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# import argparse
#
# connection = psycopg2.connect(database = 'ETL',user="postgres", password="12345",host='localhost', port=5432)
#
#
# connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
#
# cur = connection.cursor()
# # try:
# #     cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# # except:
# #     print("I can't drop our test database!")
# cur.execute("select * from test")
# print(cur.fetchall())
# connection.commit()
# connection.close()
# cur.close()
#
# cur.execute(sql.SQL("CREATE DATABASE {}").format(
#         sql.Identifier('ETL'))
#     )
#
#
# def load_data(file_path):
#     connection = psycopg2.connect(database="ETL",
#                                   host="localhost",
#                                   user="postgres",
#                                   password="",
#                                   port="5432")
#
#     cursor = connection.cursor()
#
#     print("loading data...")
#     data = extract_data(file_path)
#
#     print("transforming data...")
#     data_transform = transform_data(data)
#
#     column_name = data_transform.columns[-1]
#
#     # create table
#     query_create_table = f"CREATE TABLE IF NOT EXISTS {column_name}(\
#     ID SERIAL PRIMARY KEY,\
#     continent varchar(50) NOT NULL,\
#     country varchar(50) NOT NULL,\
#     {column_name} decimal\
#     );"
#
#     cursor.execute(query_create_table)
#
#     # start loading data
#     print('loading data...')
#     for index, row in data_transform.iterrows():
#         query_insert_value = f"INSERT INTO {column_name} (continent, country, {column_name}) VALUES ('{row[0]}', \
#             '{row[1]}', {row[2]})"
#
#         cursor.execute(query_insert_value)
#     connection.commit()
#
#     cursor.close()
#     connection.close()
#
#     print("etl success...\n")
#
#     return "all processes completed"
#
#
# if __name__ == "__main__":
#     # Initialize parser
#     parser = argparse.ArgumentParser()
#
#     # Adding optional argument
#     parser.add_argument("-f", "--file", help="file path of your dataset")
#
#     # Read arguments from command line
#     args = parser.parse_args()
#
#     load_data(args.file)