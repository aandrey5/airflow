import json
import os
import datetime as dt
import pandas as pd
import numpy as np
from modules.decor import python_operator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine, String, Integer, Float
#from decor import python_operator


@python_operator()
def connection_operator(**context):
	postg_hook = BaseHook.get_hook('airflow_dwh')
	

@python_operator()
def pushes_to_xcom (**context):
	#context['task_instance'].xcom_push('titanic', value=df_xcom)
	pass



def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@python_operator()
def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    df_xcom = df.to_json(orient='table')
    context['task_instance'].xcom_push('titanic', value=df_xcom)

   # return df_xcom

@python_operator()
def mean_fare_per_class(**context):
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    avg_df = titanic_df.groupby(['Pclass']).mean('Fare')
    avg_result = avg_df[['Fare']]
    #avg_result.to_csv(get_path('titanic_mean_fares.csv'))
    avg_xcom = avg_result.to_json(orient='table')
    context['task_instance'].xcom_push('mean_class', value=avg_xcom)




@python_operator()
def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))

@python_operator()
def pull_from_xcom(**context):
    avg_pull_xcom = context['task_instance'].xcom_pull(task_ids='mean_fare_per_class', key='mean_class')
    df_pull_xcom = context['task_instance'].xcom_pull(task_ids='download_titanic_dataset', key='titanic')
    avg_pull_xcom_out =pd.read_json(avg_pull_xcom, orient='table')
    df_pull_xcom_out = pd.read_json(df_pull_xcom, orient='table')
    avg_pull_xcom_out.to_csv(get_path('avg_pull_xcom_out.csv'))
    df_pull_xcom_out.to_csv(get_path('df_pull_xcom_out.csv'))


@python_operator()
def push_to_postgresql():
   # create sql engine for sqlalchemy
    db_string_airflow = 'postgresql://airflow_xcom:1q2w3e4r5T@192.168.147.128/data_warehouse'
    engine = create_engine(db_string_airflow)
   # read csv from previous xcom tasks 
    avg_pull_xcom_df = pd.read_csv(get_path('avg_pull_xcom_out.csv'))
    df_pull_xcom_df = pd.read_csv(get_path('df_pull_xcom_out.csv'))
   # get names of tables from variables
    avg_table = Variable.get('table_dwh_mean')
    df_table = Variable.get('table_dwh_data')
   # push data to postgresql
    avg_pull_xcom_df.to_sql(avg_table, con=engine, if_exists='replace')
    df_pull_xcom_df.to_sql(df_table, con=engine, if_exists='replace')
 


