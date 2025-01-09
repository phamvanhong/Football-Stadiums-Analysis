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
            "data/bronze/football_stadiums",
            "data/bronze/country",
            "data/bronze/continent"
        ],
        "file_names": [
            'football_stadiums.csv',
            'country.csv',
            'continent.csv'
        ]
    }
}