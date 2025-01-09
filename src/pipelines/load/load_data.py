import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import json
import pandas as pd
from datetime import datetime

def load_data(**kwargs):
    """
    Load data to the data lake
    """
    # Setup variables
    dirs = kwargs['dirs']
    file_names = kwargs['file_names']
    etl = ETL("")
    current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    layer = kwargs.get('layer', 'BRONZE')
    task_ids = {
        "BRONZE": "extract_wikipedia_data",
        "SILVER": "transform_wikipedia_data",
    }
    # Kiểm tra layer hợp lệ
    if layer not in task_ids:
        raise ValueError(f"Layer {layer} is not valid. Use 'BRONZE' or 'SILVER'.")

    for i in range(len(file_names)):
        file_name = file_names[i]
        dir = dirs[i]

        csv_filename = f"{file_name}_{current_time}.csv"
        
        data = kwargs['ti'].xcom_pull(key=file_name, 
                                    task_ids=task_ids[layer],
                                    dag_id="test_pipelines",
                                    include_prior_dates=True)
        df = pd.DataFrame(json.loads(data))
        
        etl.load(df,
                file_name=csv_filename, 
                azure_storage_key=kwargs['azure_storage_key'], 
                dir=dir,
                layer=layer)

    return "Data loaded to the data lake"