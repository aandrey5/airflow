from airflow.models import DAG
from modules.setting import default_settings
from modules.dummy import dummy


with DAG(**default_settings()) as dag:
	dummy('hello world')