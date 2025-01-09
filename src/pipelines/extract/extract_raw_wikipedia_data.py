import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL
import pandas as pd
import json
from datetime import datetime
import re

def extract_wikipedia_data(**kwargs):
    """
    Extracts data sources from the wikipedia
    """
    # Setup variables
    urls = kwargs['urls']
    target_table_index = kwargs['target_table_index']
    for url in urls:
        etl = ETL(url)

        # Extract list of tables from the webpage
        tables = etl.extract_()

        # Get the target table and convert to json
        json_target_table = tables[target_table_index].to_json(orient='records')
        kwargs['ti'].xcom_push(key=f'{url}_data', value=json_target_table)
    return "Data extracted and pushed to XCom"

