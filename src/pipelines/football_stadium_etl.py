import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import pandas as pd
import json
from datetime import datetime
import re


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
    kwargs['ti'].xcom_push(key='football_stadium_data', value=json_target_table)

    return "Data extracted and pushed to XCom"

def transform_data(**kwargs):
    """
    Transform the data
    """
    # Pull the data from XCom
    etl = ETL(kwargs['url'])
    data = kwargs['ti'].xcom_pull(key='football_stadium_data', task_ids='extract_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    # Transform the data
    football_stadium_df = etl.transform(data, 
                                        cols_drop=kwargs["cols_drop"], 
                                        cols_rename=kwargs["cols_rename"])

    # Split City column into two columns City and State/Province
    football_stadium_df[['city', 'state_or_province']] = football_stadium_df['city'].str.split(',', expand=True)

    #Hanle Null values to "Unknown" values
    football_stadium_df["state_or_province"] = football_stadium_df["state_or_province"].fillna("Unknown")

    # Remove commas from the Capacity column
    football_stadium_df['capacity'] = football_stadium_df['capacity'].str.replace(',', '', regex = True)

    # Change values in the Region column to "Asia" if they contain the word "Asia"
    football_stadium_df['continent'] = football_stadium_df['continent'].str.replace(r'.*Asia.*', 'Asia', regex=True)
    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='football_stadium_data', value=football_stadium_df.to_json(orient='records'))

    return "Data transformed and pushed to XCom"

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='football_stadium_data', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)
    etl = ETL(kwargs['url'])
    etl.load(data,
             file_name=kwargs['file_name'], 
             azure_storage_key=kwargs['azure_storage_key'],
             dir=kwargs['dir'])

