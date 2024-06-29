from metaflow import FlowSpec, step
import os
import pandas as pd
import numpy as np
import ultraimport
extract_data = ultraimport('__dir__/../ETL/extract.py', 'extract_data')
connect = ultraimport('__dir__/../ETL/connection.py', 'connect')
insert = ultraimport('__dir__/../ETL/connection.py', 'insert')

class Pipeline(FlowSpec):
    @step
    def start(self):
        self.next(self.load_data)

    @step
    def load_data(self):
        self.df = extract_data()
        self.next(self.drop_unwanted_columns)

    @step
    def drop_unwanted_columns(self):
        self.df = self.df.drop(['last_review','availability_365','name','host_name','minimum_nights'], axis=1)
        # print(self.df.isnull().sum())

        self.df['reviews_per_month'].fillna(0, inplace=True)

        self.next(self.load_in_db)

    @step
    def load_in_db(self):
        conn = connect(param_dic)
        insert(conn, df,'data')
        self.next(self.end)


    @step
    def end(self):
        print("End of Pipeline!")


if __name__ == '__main__':
    Pipeline()