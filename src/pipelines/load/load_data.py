import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL


def load_raw_data(**kwargs):
    """
    Load raw data to the bronze layer on the data lake
    """
    # Setup variables
    etl = ETL(kwargs['url'])
    data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract_raw_wikipedia_data')
    etl.load(data, 
             file_name=kwargs['file_name'], 
             azure_storage_key=kwargs['azure_storage_key'],
             dir=kwargs['dir'])
    return "Data loaded to the data lake"