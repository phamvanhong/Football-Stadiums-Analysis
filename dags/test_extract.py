import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.extract.extract_raw_wikipedia_data import extract_wikipedia_data
from airflow.models import Variable

azure_storage_key = Variable.get("azure_storage_key", default_var= None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "urls": [
                    r"https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity",
                    r"https://en.wikipedia.org/wiki/ISO_3166-1",
                    r"https://en.wikipedia.org/wiki/Template:Continent_code"
                ],
        "file_names": [
                        'football_stadiums',
                        'country',
                        'continent'
                    ],
        "target_table_indexes": [2, 1, 1],
        "azure_storage_key": azure_storage_key,
    }
}

with DAG(
    dag_id= 'test_extract',
    default_args=default_args,
    schedule_interval= None,
    catchup=False,
) as dag:
    extract_wikipedia_data = PythonOperator(
        task_id = "extract_wikipedia_data",
        python_callable= extract_wikipedia_data,
        provide_context=True
)
    
extract_wikipedia_data