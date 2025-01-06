import sys
sys.path.insert(0, '/opt/airflow/src/')
from airflow.models import Variable
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import json
from objects.etl import ETL

default_args = {
    'owner': 'Hong Pham Van',
    'start_date': datetime(2024, 12, 22),
    "op_kwargs": {
        "url": " ",
        "file_name": ('ready_football_stadiums_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv'),
        "dir": "ready/",
        "azure_storage_key": Variable.get("azure_storage_key", default_var=None),
    }

}

with DAG(
    dag_id='transforms_to_ready',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    def transform_data(**kwargs):
        """
        Transform the data
        """
        # Pull the data from XCom
        continent_data = kwargs['ti'].xcom_pull(key='continent_codes_data', 
                                        task_ids='transform_wikipedia_data', 
                                        dag_id='continent_codes_flow',
                                        include_prior_dates=True)
        continent_df = pd.DataFrame(json.loads(continent_data))

        country_data = kwargs['ti'].xcom_pull(key='country_codes_data', 
                                           task_ids='transform_wikipedia_data',
                                           dag_id='country_codes_flow',
                                           include_prior_dates=True)
        country_df = pd.DataFrame(json.loads(country_data))

        football_stadiums_data = kwargs['ti'].xcom_pull(key='football_stadiums_data',
                                                    task_ids='transform_wikipedia_data',
                                                    dag_id='football_stadiums_flow',
                                                    include_prior_dates=True)
        football_stadiums_df = pd.DataFrame(json.loads(football_stadiums_data))

        # Transform the data
        first_merged = football_stadiums_df.merge(country_df, on='country', how='left')

        # encode the rest of the countries that are not in the country_df
        country_to_id = {
                        'Netherlands': "NL",
                        'Venezuela': "VE",
                        'Tanzania': "TZ",
                        'Wales': "WAL",
                        'Russia': "RU",
                        'Scotland': "SCO",
                        'Syria': "SY",
                        'Turkey': "TR",
                        'Taiwan': "TW",
                        'United States': "US",
                        'Iran': "IR",
                        'China': "CN",
                        'South Korea': "KR",
                        'Vietnam': "VN",
                        'England': "ENG",
                        'Bolivia': "BO",
                        'DR Congo': "CG",
                        'North Korea': "KP"
                    }
        # Mapping the country to the country_id
        first_merged['country_id'] = first_merged.apply(lambda row: country_to_id[row['country']] if pd.isnull(row['country_id']) and row['country'] in country_to_id else row['country_id'], axis=1)

        final_merged = first_merged.merge(continent_df, on='continent', how='left')

        kwargs['ti'].xcom_push(key='ready_football_stadiums', value=final_merged.to_json(orient='records'))
        return "Data transformed and pushed to XCom"

    def load_data(**kwargs):
        data = kwargs['ti'].xcom_pull(key='ready_football_stadiums', task_ids='transform_to_ready')
        data = pd.DataFrame(json.loads(data))
        etl = ETL(kwargs['url'])
        etl.load(data,
                 file_name=kwargs['file_name'],
                 azure_storage_key=kwargs['azure_storage_key'],
                 dir=kwargs['dir'])

    transforms_to_ready = PythonOperator(
        task_id = "transform_to_ready",
        python_callable= transform_data,
        provide_context=True
    )

    load_data_to_ready = PythonOperator(
        task_id = "load_data_to_ready",
        python_callable= load_data,
        provide_context=True
    )

    transforms_to_ready >> load_data_to_ready