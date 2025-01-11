import sys
sys.path.insert(0, '/opt/airflow/')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.common.constants import *
from airflow.models import Variable


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
        ]
    }
}

def write_data_to_local(**kwargs):
    """
    Write data to local storage
    """
    # Setup variables
    file_names = kwargs[FILE_NAMES]
    dirs = kwargs[DIRS]

    for i in range(len(file_names)):
        # Pull transformed data from the previous task
        transformed_data = kwargs[TI].xcom_pull(key=file_names[i],
                                      task_ids=TRANSFORM_EXTRACTED_DATA,
                                      dag_id=ETL_FLOW,
                                      include_prior_dates=True)
        raw_data = kwargs[TI].xcom_pull(key=file_names[i],
                                      task_ids=EXTRACT_WIKIPEDIA_DATA,
                                      dag_id=ETL_FLOW,
                                      include_prior_dates=True)
        
        # Write the data to BRONZE layer on local storage
        with open(f"data/{BRONZE}/{dirs[i]}{file_names[i]}.json", 'w') as f:
            f.write(raw_data)

        # Write the data to SILVER layer on local storage
        with open(f"data/{SILVER}/{dirs[i]}{file_names[i]}.json", 'w') as f:
            f.write(transformed_data)

with DAG(
    dag_id = 'write_data_to_local',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    write_data_to_local = PythonOperator(
        task_id='write_data_to_local',
        python_callable=write_data_to_local,
        provide_context=True
    )
    write_data_to_local