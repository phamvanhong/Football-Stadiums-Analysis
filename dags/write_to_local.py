import sys
sys.path.insert(0, '/opt/airflow/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import *
from airflow.models import Variable

azure_storage_key = Variable.get(AZURE_STORAGE_KEY, default_var=None)



default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    OP_KWARGS: {
        FILE_NAMES: [
                        FOOTBALL_STADIUMS,
                        COUNTRY,
                        CONTINENT
                    ],
        DIRS: [
            "football_stadiums/",
            "country/",
            "continent/"
        ],
        AZURE_STORAGE_KEY: azure_storage_key,
    }
}

def write_data_to_local(**kwargs):
    """
    Write data to local storage
    """
    # Setup variables
    file_names = kwargs[FILE_NAMES]
    dirs = kwargs[DIRS]
    azure_storage_key = kwargs[AZURE_STORAGE_KEY]

    # Load the data
    etl = ETL(EMPTY_STRING)
    data = etl.extract()

    # Transform the data
    data = etl.transform(data, cols_drop=kwargs[COLS_DROP], cols_rename=kwargs[COLS_RENAME])

    # Load the data
    etl.load(data, file_names, azure_storage_key, dirs, LAYER_RAW)
with DAG(
    'write_data_to_local',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_wikipedia_data >> transform_extracted_data >> load_data