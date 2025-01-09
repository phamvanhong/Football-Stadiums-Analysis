import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import json
import pandas as pd
from datetime import datetime

def load_raw_data(**kwargs):
    """
    Load data to the data lake
    """
    # Setup variables
    url = ""
    dirs = kwargs['dirs']
    file_names = kwargs['file_names']
    etl = ETL(url)
    current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")

    for i in range(len(file_names)):
        file_name = file_names[i]
        dir = dirs[i]

        csv_filename = f"{file_name}_{current_time}.csv"

        data = kwargs['ti'].xcom_pull(key=file_name, 
                                      task_ids='extract_wikipedia_data',
                                      dag_id='test_extract',
                                      include_prior_dates=True)
        df = pd.DataFrame(json.loads(data))
        
        etl.load(df,
                 file_name=csv_filename, 
                 azure_storage_key=kwargs['azure_storage_key'], 
                 dir=dir)
    return "Data loaded to the data lake"