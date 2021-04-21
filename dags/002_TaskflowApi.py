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
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        df.to_csv(get_path('titanic.csv'), encoding='utf-8')
        df_xcom = df.to_json(orient='table')
        #context['task_instance'].xcom_push('titanic', value=df_xcom)
        dataset_df = df_xcom
        return dataset_df

    @task()
    def mean_fare_per_class():
        titanic_df = pd.read_csv(get_path('titanic.csv'))
        avg_df = titanic_df.groupby(['Pclass']).mean('Fare')
        avg_result = avg_df[['Fare']]
        #avg_result.to_csv(get_path('titanic_mean_fares.csv'))
        avg_xcom = avg_result.to_json(orient='table')
        #context['task_instance'].xcom_push('mean_class', value=avg_xcom)
        dataset_mean = avg_xcom
        return dataset_mean

    @task()
    def pivot_dataset():
        titanic_df = pd.read_csv(get_path('titanic.csv'))
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()
        #df.to_csv(get_path('titanic_pivot.csv'))
        dataset_pivot = df.to_json(orient='table')
        return dataset_pivot


    @task()
    def push_to_postgresql(data):
       # create sql engine for sqlalchemy
        db_string_airflow = 'postgresql://airflow_xcom:1q2w3e4r5T@10.128.100.98/dwh'
        engine = create_engine(db_string_airflow)
       # read dataframes from previous returned json data  
        avg_pull_xcom_df = pd.read_json(data, orient='table')
       # df_pull_xcom_df = pd.read_json(dataset_df, orient='table')
       # pivot_pull_xcom_df = pd.read_json(dataset_pivot, orient='table')
       # get names of tables from variables
       # avg_table = Variable.get(f'{data}')
       # df_table = Variable.get('table_dwh_data')
       # pivot_table = Variable.get('table_dwh_pivot')
    
       # push data to postgresql
        avg_pull_xcom_df.to_sql(f'data', con=engine, if_exists='replace')
       # df_pull_xcom_df.to_sql(df_table, con=engine, if_exists='replace')
       # pivot_pull_xcom_df.to_sql(pivot_table, con=engine, if_exists='replace')

    
    dataset_df =  download_titanic_dataset()
    push_df =  push_to_postgresql(dataset_df)

    dataset_mean  = mean_fare_per_class()
    push_mean = push_to_postgresql(dataset_mean)
    
    dataset_pivot = pivot_dataset()
    push_pivot = push_to_postgresql(dataset_pivot)

    
Taskflow = TaskFlowApi()
