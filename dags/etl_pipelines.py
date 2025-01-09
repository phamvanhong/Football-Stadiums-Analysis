import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.load.load_data import load_data
from pipelines.extract.extract_wikipedia_data import extract_wikipedia_data
from pipelines.transform.transform_data import transform_extracted_data
from common.constants import *
from airflow.models import Variable



azure_storage_key = Variable.get(AZURE_STORAGE_KEY, default_var=None)

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    OP_KWARGS: {
        URLS: [
                    r"https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity",
                    r"https://en.wikipedia.org/wiki/ISO_3166-1",
                    r"https://en.wikipedia.org/wiki/Template:Continent_code"
                ],
        FILE_NAMES: [
                        FOOTBALL_STADIUMS,
                        COUNTRY,
                        CONTINENT
                    ],
        TARGET_TABLE_INDEXES: [2, 1, 1],
        COLS_DROP: [
            ["Images"],
            ["Alpha-3 code", "Link to ISO 3166-2", "Independent[b]", "Numeric code"],
            ["Markup"]
        ],
        COLS_RENAME: [
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
        ],
        DIRS: [
            "football_stadiums/",
            "country/",
            "continent/"
        ],
        AZURE_STORAGE_KEY: azure_storage_key,
    }
}

with DAG(
    dag_id=ETL_FLOW,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    extract_wikipedia_data = PythonOperator(
        task_id=EXTRACT_WIKIPEDIA_DATA,
        python_callable=extract_wikipedia_data,
        provide_context=True
    )
    transform_wikipedia_data = PythonOperator(
        task_id=TRANSFORM_EXTRACTED_DATA,
        python_callable=transform_extracted_data,
        provide_context=True
    )
    load_to_bronze = PythonOperator(
        task_id=LOAD_TO_BRONZE,
        python_callable=load_data,
        op_kwargs={
            LAYER: BRONZE,
            DIRS: default_args[OP_KWARGS][DIRS],
            FILE_NAMES: default_args[OP_KWARGS][FILE_NAMES],
            AZURE_STORAGE_KEY: azure_storage_key},
        provide_context=True
    )
    load_to_silver = PythonOperator(
        task_id=LOAD_TO_SILVER,
        python_callable=load_data,
        op_kwargs={
            LAYER: SILVER,
            DIRS: default_args[OP_KWARGS][DIRS], 
            FILE_NAMES: default_args[OP_KWARGS][FILE_NAMES],
            AZURE_STORAGE_KEY: azure_storage_key
        },
        provide_context=True
    )
    extract_wikipedia_data >> load_to_bronze >> transform_wikipedia_data >> load_to_silver