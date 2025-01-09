import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.load.load_data import load_raw_data
from airflow.models import Variable

azure_storage_key = Variable.get("azure_storage_key", default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "url": " ",
        "azure_storage_key": azure_storage_key,
        "dirs": [
            "bronze/football_stadiums/",
            "bronze/country/",
            "bronze/continent/"
        ],
        "file_names": [
            'football_stadiums_',
            'country_',
            'continent_'
        ],
        "keys": [
            "football_stadiums_data",
            "country_data",
            "continent_data"
        ]
    }
}

with DAG(
    dag_id='test_load',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
        provide_context=True
    )
    load_raw_data