import sys
sys.path.insert(0, '/opt/airflow/src/')
from airflow.models import Variable
from pipelines.country_codes_etl import extract_data, transform_data, load_data
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime


azure_storage_key = Variable.get("azure_storage_key", default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "url": r'https://en.wikipedia.org/wiki/ISO_3166-1',
        "cols_drop":
        [
            "Alpha-3 code",
            "Link to ISO 3166-2",
            "Independent[b]"
        ],
        "cols_rename":
        {
            "english short name (using title case)": "country",
            "alpha-2 code": "country_id", 
            "numeric code": "country_numeric_id"
        },
        "target_table_index": 1,
        "file_name": ('country_codes_' + str(datetime.now().date())
                      + "_" + str(datetime.now().time()).replace(":", "_") + '.csv'),
        "dir": "raw_data/country_codes_data",
        "azure_storage_key": azure_storage_key,
    }

}

with DAG(
    dag_id='country_codes_flow',
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
