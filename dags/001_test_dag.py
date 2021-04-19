from airflow.models import DAG
from modules.setting import default_settings
from modules.dummy import dummy
from modules.funct import pull_from_xcom,  pushes_to_xcom,  get_path, download_titanic_dataset, mean_fare_per_class, pivot_dataset, connection_operator



titanic_dataset_operator  = download_titanic_dataset()

with DAG(**default_settings()) as dag:
	titanic_dataset_operator  = download_titanic_dataset()
	#titanic_dataset_operator >> pivot_dataset()
#	download_titanic_dataset()  >> pivot_dataset()
	#titanic_dataset_operator  >> mean_fare_per_class()
	#connection_operator() >> pushes_to_xcom()
	titanic_dataset_operator >> mean_fare_per_class() >> pull_from_xcom()

