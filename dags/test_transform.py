import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.transform.transform_data import transform_data
from airflow.models import Variable

azure_storage_key = Variable.get("azure_storage_key", default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "keys": [
            "football_stadiums_data",
            "country_data",
            "continent_data"
        ],
        "cols_drop": [
            ["Images"],
            ["Alpha-3 code", "Link to ISO 3166-2", "Independent[b]", "Numeric code"],
            ["Markup"]
        ],
        "cols_rename": [
            {
                "rank": "stadium_id", 
                "home team(s)": "home_teams", 
                "seating capacity": "capacity",
                "region": "continent",
            },
            {
                "english short name (using title case)": "country",
                "alpha-2 code": "country_id", 
            },
            {
                "output": "continent_id",
            }
        ]
    }
}

with DAG(
    dag_id='test_transform',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )
    transform_data