from metaflow import FlowSpec, step
import os
import pandas as pd
import numpy as np
import ultraimport
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import seaborn as sns
connect_with_db = ultraimport('__dir__/../ETL/connection.py', 'connect_with_db')
load_from_db = ultraimport('__dir__/../ETL/connection.py', 'load_from_db')
insert = ultraimport('__dir__/../ETL/connection.py', 'insert')
class Pipeline3(FlowSpec):
    @step
    def start(self):
        self.next(self.fetch_raw)

    @step
    def fetch_raw(self):
        conn = connect_with_db()
        self.df = load_from_db(conn)
        self.next(self.plot1)

    @step
    def plot1(self):
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=self.df.longitude, y=df.latitude, hue=df.neighbourhood_group)
        plt.savefig('/home/ateefa/Project/Demo/Data/Output/neighborhood_distribution.pdf')
        self.next(self.plot2)

    @step
    def plot2(self):
        avg_price = self.df.groupby(['neighbourhood_group', 'room_type'])['price'].mean()
        print(avg_price)
        avg_price_df = avg_price.reset_index()
        plt.figure(figsize=(12, 8))
        sns.barplot(x='neighbourhood_group', y='price', hue='room_type', data=avg_price_df, palette='viridis')
        plt.title('Average Price of Room Types by Neighbourhood Group', fontsize=16)
        plt.xlabel('Neighbourhood Group', fontsize=14)
        plt.ylabel('Average Price', fontsize=14)
        plt.xticks(rotation=45, fontsize=12)
        plt.tight_layout()
        plt.savefig('/home/ateefa/Project/Demo/Data/Output/avg_price.pdf')
        self.next(self.plot3)
    def plot3(self):
        plt.scatter(self.df["neighbourhood_group"], df["price"])
        plt.savefig('/home/ateefa/Project/Demo/Data/Output/grp_price.pdf')
        self.next(self.end)
    @step
    def end(self):
        print("End of Pipeline!")


if __name__ == '__main__':
    Pipeline3()