import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.web_scraping import WebScraping
from common.constants import *
import pandas as pd
import re


class ETL:
    """
    Initialize the ETL object
    """
    def __init__(self, url: str) -> None:
        self.url = url
    
    def extract(self):
        """
        Extracts list of tables from the webpage
        """
        # Setup variables
        target_table = []
        ws = WebScraping(self.url)
        table_elements = ws.get_html_element(TABLE)

        for table in table_elements:
            target_table.append(pd.read_html(str(table))[0])
        return target_table
    
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform the data

        Args:
            data: pd.DataFrame - the data to transform
            **kwargs - additional arguments
        Returns:
            pd.DataFrame - the transformed data
        """
        # Handle duplicates
        data = data.drop_duplicates().reset_index(drop=True)

        # Drop unnecessary columns
        data = data.drop(kwargs['cols_drop'], axis = 1)

        # Remove special characters
        data = data.apply(lambda col: col.map(lambda x: re.sub(r"[♦]|\[\d+\]|\[\w+\]", "", x) if isinstance(x, str) else x))

        # rename columns
        data.columns = [col.lower() for col in data.columns]
        data = data.rename(columns=kwargs[COLS_RENAME])

        return data
    
    def load(self, data: pd.DataFrame, 
             file_name: str, 
             azure_storage_key: str, 
             dir: str,
             layer: str) -> str:
        """
        Load the data to the data lake

        Args:
            data: pd.DataFrame - the data to load
            file_name: str - the name of the file
            azure_storage_key: str - the azure storage key
            dir: str - the directory to store the data
            layer: str - the layer of the data lake
        """
        # abfs://<container>@<storage-account>.dfs.core.windows.net/<directory>/<file>
        path = f'abfs://footballstadiums@footballstadiumsdata.dfs.core.windows.net/data/{layer}/{dir}/{file_name}'
        
        data.to_json(path,
                storage_options={
                ACCOUNT_KEY : azure_storage_key
                }, orient=RECORDS)
        return "Data loaded to the data lake"