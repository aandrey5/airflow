
#!/usr/bin/env bash

export AIRFLOW_HOME=$PWD
export PYTHONPATH=$PWD/lib

airflow scheduler & sleep 5 && airflow webserver -p 8080 && fg