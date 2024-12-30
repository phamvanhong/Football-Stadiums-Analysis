import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import pandas as pd
import json
import re
from datetime import datetime

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
    football_stadium_df[['City', 'State_or_Province']] = football_stadium_df['City'].str.split(',', expand=True)

    #Hanle Null values to "Unknown" values
    football_stadium_df["State_or_Province"] = football_stadium_df["State_or_Province"].fillna("Unknown")

    # Remove commas from the Capacity column
    football_stadium_df['Capacity'] = football_stadium_df['Capacity'].str.replace(',', '', regex = True)

    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='football_stadium_data', value=football_stadium_df.to_json(orient='records'))

    return "Data transformed and pushed to XCom"

# def load_data(**kwargs):
#     """
#     Load the data to the database
#     """
#     # Pull the data from XCom
#     data = json.loads(kwargs['ti'].xcom_pull(key='football_stadium_data', task_ids='transform_wikipedia_data'))
#     football_stadium_df = pd.DataFrame(data)

#     etl = ETL(kwargs['url'])
#     etl.load(football_stadium_df, 
#              folder_name = kwargs['folder_name'], 
#              file_name = kwargs['file_name'],
#              azure_storage_key = kwargs['azure_storage_key'])

#     return "Data loaded to the database"

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='football_stadium_data', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)
    path = f'abfs://footballstadiumdataeng@footballstadiumdataeng.dfs.core.windows.net/raw_data/{kwargs["folder_name"]}/{kwargs["file_name"]}'

    # data.to_csv('data/' + file_name, index=False)
    data.to_csv(path,
                storage_options={
                    'account_key': kwargs['azure_storage_key']
                }, index=False)

