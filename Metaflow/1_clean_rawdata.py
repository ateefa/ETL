from metaflow import FlowSpec, step
import os
import pandas as pd
import numpy as np
import ultraimport
extract_data = ultraimport('__dir__/../ETL/extract.py', 'extract_data')

class Pipeline(FlowSpec):
    @step
    def start(self):
        self.next(self.load_data)

    @step
    def load_data(self):
        df = extract_data()
        print(df)
        self.next(self.end)

    @step
    def end(self):
        print("End of Pipeline!")


if __name__ == '__main__':
    Pipeline()