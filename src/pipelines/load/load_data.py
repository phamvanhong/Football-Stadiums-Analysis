import sys
sys.path.insert(0, '/opt/airflow/src/')
from datetime import datetime
import pandas as pd
import json
from common.constants import *
from objects.etl import ETL


def load_data(**kwargs) -> str:
    """
    Load data to the different layers of the data lake

    Args:
        **kwargs - additional arguments
    Returns:
        str - a message
    """
    # Setup variables
    dirs = kwargs[DIRS]
    file_names = kwargs[FILE_NAMES]
    etl = ETL(EMPTY_STRING)
    current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    layer = kwargs.get(LAYER, BRONZE)
    task_ids = {
        BRONZE: EXTRACT_WIKIPEDIA_DATA,
        SILVER: TRANSFORM_EXTRACTED_DATA,
    }

    for i in range(len(file_names)):
        file_name = file_names[i]
        dir = dirs[i]

        csv_filename = f"{file_name}_{current_time}.{CSV}"

        data = kwargs[TI].xcom_pull(key=file_name,
                                    task_ids=task_ids[layer],
                                    dag_id=ETL_FLOW,
                                    include_prior_dates=True)
        df = pd.DataFrame(json.loads(data))

        etl.load(df,
                 file_name=csv_filename,
                 azure_storage_key=kwargs[AZURE_STORAGE_KEY],
                 dir=dir,
                 layer=layer)

    return "Data loaded to the data lake"
