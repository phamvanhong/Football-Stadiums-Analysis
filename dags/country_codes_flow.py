import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.country_codes_etl import extract_data
from airflow.models import Variable

azure_storage_key = Variable.get("azure_storage_key", default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "url": r'https://en.wikipedia.org/wiki/ISO_3166-1',
        "cols_drop": ["Images"],
        "cols_rename": {"Rank": "Stadium_id", "Home team(s)": "Home_teams", "Seating capacity": "Capacity"},
        "target_table_index": 0,
        "file_name": ('football_stadiums_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv'),
        "dir": "raw_data/football_stadiums_data",
        "azure_storage_key": azure_storage_key,
    }
    
}

with DAG(
    dag_id= 'country_codes_flow',
    default_args=default_args,
    schedule_interval= None,
    catchup=False,
) as dag:
    extract_wikipedia_data = PythonOperator(
        task_id = "extract_wikipedia_data",
        python_callable= extract_data,
        provide_context=True
    )
    extract_wikipedia_data


