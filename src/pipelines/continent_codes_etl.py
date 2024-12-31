import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import pandas as pd
import json


def extract_data(**kwargs):
    """
    Extracts data from the webpage
    """
    # Setup variables
    url = kwargs['url']
    target_table_index = kwargs['target_table_index']
    etl = ETL(url)

    # Extract list of tables from the webpage
    tables = etl.extract_()

    # Get the target table and convert to json
    json_target_table = tables[target_table_index].to_json(orient='records')
    kwargs['ti'].xcom_push(key='continent_codes_data', value=json_target_table)

    return "Data extracted and pushed to XCom"

def transform_data(**kwargs):
    """
    Transform the data
    """
    # Pull the data from XCom
    etl = ETL(kwargs['url'])
    data = kwargs['ti'].xcom_pull(key='continent_codes_data', task_ids='extract_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    # Transform the data
    football_stadium_df = etl.transform(data, 
                                        cols_drop=kwargs["cols_drop"], 
                                        cols_rename=kwargs["cols_rename"])
    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='continent_codes_data', value=football_stadium_df.to_json(orient='records'))

    return "Data transformed and pushed to XCom"

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='continent_codes_data', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)
    etl = ETL(kwargs['url'])
    etl.load(data,
             file_name=kwargs['file_name'], 
             azure_storage_key=kwargs['azure_storage_key'],
             dir=kwargs['dir'])

