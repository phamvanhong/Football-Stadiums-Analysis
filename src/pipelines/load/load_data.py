import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import json
import pandas as pd


def load_raw_data(**kwargs):
    """
    Load raw data to the BRONZE layer on the data lake
    """
    # Setup variables
    keys = kwargs['keys']
    url = kwargs['url']
    dirs = kwargs['dirs']
    file_names = kwargs['file_names']

    etl = ETL(url)

    for i in range(len(keys)):
        key = keys[i]
        dir = dirs[i]
        file_name = file_names[i]

        data = kwargs['ti'].xcom_pull(key=key, task_ids='extract_wikipedia_data')
        data = pd.DataFrame(json.loads(data))
        
        etl.load(data, 
                 file_name=file_name, 
                 azure_storage_key=kwargs['azure_storage_key'], 
                 dir=dir)
    return "Data loaded to the data lake"
