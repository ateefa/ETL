from metaflow import FlowSpec, step
import os
import pandas as pd
import numpy as np
import ultraimport
import datetime as dt
connect_with_db = ultraimport('__dir__/../ETL/connection.py', 'connect_with_db')
load_from_db = ultraimport('__dir__/../ETL/connection.py', 'load_from_db')
insert = ultraimport('__dir__/../ETL/connection.py', 'insert')
class Pipeline2(FlowSpec):
    @step
    def start(self):
        self.next(self.fetch_raw)

    @step
    def fetch_raw(self):
        conn = connect_with_db()
        self.df = load_from_db(conn)
        self.next(self.normalize)

    @step
    def normalize(self):
        self.df['last_updated'] = dt.datetime.today().strftime("%m/%d/%Y")
        self.df['Year'] = pd.to_datetime(self.df['last_updated']).dt.year
        self.df['Month'] = pd.to_datetime(self.df['last_updated']).dt.month
        self.next(self.additional_metrics)

    @step
    def additional_metrics(self):
        n_count = self.df['neighbourhood_group'].value_counts()
        latest_yr_count = self.df.where(self.df['Year'] == 2024)
        bnb_booked = latest_yr_count['neighbourhood_group'].value_counts()
        nghb = self.df['neighbourhood'].value_counts()

        self.n_count_df = n_count.rename_axis('unique_values').reset_index(name='counts')
        self.bnb_booked_df = bnb_booked.rename_axis('unique_values').reset_index(name='counts')
        self.nghb_df = nghb.rename_axis('unique_values').reset_index(name='counts')
        print(n_count)
        print(bnb_booked)
        print(nghb)

        ### AVG PRICE PER NEIG
        avg_price = self.df.groupby('neighbourhood')['price'].agg(np.mean)
        print(avg_price)

        self.next(self.put_new_metrics_in_db)
    def put_new_metrics_in_db(self):
        conn = connect_with_db()
        insert(conn, self.n_count_df, 'n_count')
        insert(conn, self.n_count_df, 'bnb_booked')
        insert(conn, self.n_count_df, 'nghb')
        self.next(self.end)
    @step
    def end(self):
        print("End of Pipeline!")


if __name__ == '__main__':
    Pipeline2()