import sys
sys.path.insert(0, '/opt/airflow/src/')
from airflow.models import Variable
from pipelines.continent_codes_etl import extract_data, transform_data, load_data
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

#Nếu không thể load, thì phải vào airflow cài lại azure_storage_key
azure_storage_key = Variable.get("azure_storage_key", default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "url": r'https://en.wikipedia.org/wiki/Template:Continent_code',
        "cols_drop":
        [
            "Markup"
        ],
        "cols_rename":
        {
            "output": "continent_id",
        },
        "target_table_index": 1,
        "file_name": ('continent_codes_' + str(datetime.now().date())
                      + "_" + str(datetime.now().time()).replace(":", "_") + '.csv'),
        "dir": "raw/continent",
        "azure_storage_key": azure_storage_key,
    }

}

with DAG(
    dag_id='continent_codes_flow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    extract_wikipedia_data = PythonOperator(
        task_id="extract_wikipedia_data",
        python_callable=extract_data,
        provide_context=True
    )
    transform_wikipedia_data = PythonOperator(
        task_id = "transform_wikipedia_data",
        python_callable= transform_data,
        provide_context=True
    )

    load_wikipedia_data = PythonOperator(
        task_id = "load_wikipedia_data",
        python_callable= load_data,
        provide_context=True
    )
    extract_wikipedia_data >> transform_wikipedia_data >> load_wikipedia_data