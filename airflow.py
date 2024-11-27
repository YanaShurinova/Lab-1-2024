from __future__ import annotations
import os
import io
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from elasticsearch import Elasticsearch
from pandas import DataFrame

log = logging.getLogger(__name__)

with DAG(
        dag_id="file_processing",
        schedule=None,
        catchup=False,
        tags=["example"],
        start_date=datetime(year=2024, month=10, day=10, hour=20, minute=20, second=20)
) as dag:
    @task(task_id="read_file")
    def read_file(file_path: str):
        with open(file_path) as file_:
            return file_.read()


    @task(task_id="divide_file")
    def divide_file(data: str):
        return pd.read_csv(io.StringIO(data), sep=',')


    @task(task_id="filter_data")
    def filter_data(data: DataFrame):
        data = data[data['designation'].notna()]
        return data[data['region_1'].notna()]


    @task(task_id="transform_price")
    def transform_price(data: DataFrame):
        data['price'] = data['price'].replace({np.nan: 0.0})
        return data


    @task(task_id="save_to_result_table")
    def save_to_result_table(data: DataFrame, output_path: str = "result_table.csv"):
        result_table = pd.read_csv(output_path)
        result_table = pd.concat([result_table, data])
        result_table.to_csv(output_path)


    @task(task_id="save_to_elasticsearch")
    def save_to_elasticsearch(data: DataFrame, path_: str):
        client = Elasticsearch(hosts=["http://elasticsearch-kibana:9200"])
        client.indices.create(index="my_index")
        for string in data.to_dict('index'):
            client.update(index="my_index", id=path_, doc=string)


    for path in os.listdir("data"):
        csv_file = read_file(f"data/{path}")
        df = divide_file(csv_file)
        filtered_data = filter_data(df)
        transformed_price = transform_price(filtered_data)
        save_to_result_table(transformed_price)
        save_to_elasticsearch(transformed_price, path)
