import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
from common.constants import *


def extract_wikipedia_data(**kwargs) -> str:
    """
    Extracts data sources from the wikipedia

    Args:
        **kwargs - additional arguments
    Returns:
        str - a message
    """
    # Setup variables
    urls = kwargs[URLS]
    target_table_indexes = kwargs[TARGET_TABLE_INDEXES]
    file_names = kwargs[FILE_NAMES]
    for i in range(len(urls)):
        url = urls[i]
        target_table_index = target_table_indexes[i]
        file_name = file_names[i]
    
        # ETL process
        etl = ETL(url) 
        tables = etl.extract()  # Extract all tables from the html
    
        json_target_table = tables[target_table_index].to_json(orient=RECORDS) # Convert target table to json
        kwargs[TI].xcom_push(key=f'{file_name}', value=json_target_table)
    return "Data extracted and pushed to XCom"


