import os
import datetime as dt
import pandas as pd
import numpy as np
from modules.decor import python_operator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
#from decor import python_operator


@python_operator()
def connection_operator(**context):
	postg_hook = BaseHook.get_hook('airflow_dwh')
	import pdb; pdb.set_trace()


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@python_operator()
def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')

@python_operator()
def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    avg_df = titanic_df.groupby(['Pclass']).mean('Fare')
    avg_result = avg_df[['Fare']]
    avg_result.to_csv(get_path('titanic_mean_fares.csv'))

@python_operator()
def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))

