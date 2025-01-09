import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.web_scraping import WebScraping
import pandas as pd
import re


class ETL:
    def __init__(self, url):
        self.url = url
    
    def extract_(self):
        """
        Extracts list of tables from the webpage
        """
        # Setup variables
        target_table = []
        ws = WebScraping(self.url)
        table_elements = ws.get_html_element('table')

        for table in table_elements:
            target_table.append(pd.read_html(str(table))[0])
        return target_table
    
    def transform(self, data: pd.DataFrame, **kwargs):
        """
        Transform the data
        """
        # Handle duplicates
        data = data.drop_duplicates().reset_index(drop=True)

        # Drop unnecessary columns
        data = data.drop(kwargs['cols_drop'], axis = 1)

        # Remove special characters except comma, space, "-", and "'"
        data = data.apply(lambda col: col.map(lambda x: re.sub(r"[^\w,\-\s']", "", x, flags=re.UNICODE) if isinstance(x, str) else x))

        # rename columns
        data.columns = [col.lower() for col in data.columns]
        data = data.rename(columns=kwargs['cols_rename'])

        return data
    
    def load(self, data: pd.DataFrame, **kwargs):
        """
        Load the data to the database
        """
        file_name = kwargs['file_name']
        account_key = kwargs['azure_storage_key']
        dir = kwargs['dir']
        
        # abfs://<container>@<storage-account>.dfs.core.windows.net/<directory>/<file>
        path = f'abfs://footballstadiums@footballstadiumsdata.dfs.core.windows.net/{dir}/{file_name}'
        
        data.to_csv(path,
                storage_options={
                    'account_key': account_key
                }, index=False)
        return "Data loaded to the database"