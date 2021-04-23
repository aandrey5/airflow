from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json
import os
import datetime as dt
import pandas as pd
import numpy as np
from airflow.models import Variable
from sqlalchemy import create_engine, String, Integer, Float

@dag(default_args={'owner': 'MorozovAN'}, schedule_interval=None, start_date=days_ago(2))
def TaskFlowApi():
    
    def get_path(file_name):
        return os.path.join(os.path.expanduser('~'), file_name)
   
    @task()
    def download_titanic_dataset():
        if get_path('titanic.csv'):
            url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
            df = pd.read_csv(url)
            df.to_csv(get_path('titanic.csv'), encoding='utf-8')
        else: df = pd.read_csv(get_path('titanic.csv'), encoding='utf-8')

        df_xcom = df.to_json(orient='table')
        dataset_df = df_xcom
        return dataset_df

    @task()
    def mean_fare_per_class(data):
        titanic_df = pd.read_json(data, orient='table')
        avg_df = titanic_df.groupby(['Pclass']).mean('Fare')
        avg_result = avg_df[['Fare']]
        avg_xcom = avg_result.to_json(orient='table')
        dataset_mean = avg_xcom
        
        return dataset_mean

    @task()
    def pivot_dataset(data):
        titanic_df = pd.read_json(data, orient='table')
        titanic_df['Pclass'] = titanic_df['Pclass'].astype(str)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()
      
        df = df.fillna(0)
       # titanic_df.to_csv(get_path('pivot_control.csv'), encoding='utf-8')
        dataset_pivot = df.to_json(orient='table')
        return dataset_pivot


    @task()
    def push_to_postgresql(name, data):
       # create sql engine for sqlalchemy
        db_string_airflow = 'postgresql://airflow_xcom:1q2w3e4r5T@10.128.100.98/dwh'
        engine = create_engine(db_string_airflow)
       # read dataframes from previous returned json data  
        avg_pull_xcom_df = pd.read_json(data, orient='table')
       # push data to postgresql
        avg_pull_xcom_df.to_sql(name, con=engine, if_exists='replace')
       

    
    dataset_df =  download_titanic_dataset()
    push_df =  push_to_postgresql('dataset_df',dataset_df)

    dataset_mean  = mean_fare_per_class(dataset_df)
    push_mean = push_to_postgresql('dataset_mean', dataset_mean)
    
    dataset_pivot = pivot_dataset(dataset_df)
    push_pivot = push_to_postgresql('dataset_pivot', dataset_pivot)

    
Taskflow = TaskFlowApi()
