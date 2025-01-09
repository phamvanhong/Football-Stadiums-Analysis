import sys
sys.path.insert(0, '/opt/airflow/src/')
import json
import pandas as pd
from objects.etl import ETL


def transform_football_stadiums_data(dataframe: pd.DataFrame):
    """
    Transform the data
    """
    # Transform the data
    football_stadium_df = dataframe
    # Remove commas from the Capacity column
    football_stadium_df['capacity'] = football_stadium_df['capacity'].str.replace(
        ',', '', regex=True)

    # Change values in the Region column to "Asia" if they contain the word "Asia"
    football_stadium_df['continent'] = football_stadium_df['continent'].str.replace(
        r'.*Asia.*', 'Asia', regex=True)
    return football_stadium_df


def transform_continent_data(dataframe: pd.DataFrame):
    """
    Transform the data
    """
    continent_df = dataframe
    continent_df['continent_id'] = continent_df['continent_id'].fillna("NA")
    return continent_df


def transform_data(**kwargs):
    """
    Transform the data
    """
    # Setup variables
    file_names = kwargs['file_names']
    cols_drop = kwargs['cols_drop']
    cols_rename = kwargs['cols_rename']
    url = ""
    etl = ETL(url)

    # Transform each dataset
    for i in range(len(file_names)):
        data = kwargs['ti'].xcom_pull(key=file_names[i],
                                      task_ids='extract_wikipedia_data',
                                      dag_id='test_extract',
                                      include_prior_dates=True)
        df = pd.DataFrame(json.loads(data))
  
        # Basic transformation for all datasets
        df = etl.transform(df,
                           cols_drop=cols_drop[i],
                           cols_rename=cols_rename[i])
        
        # Additional transformation for specific datasets
        if file_names[i] == 'football_stadiums_data':
            df = transform_football_stadiums_data(df)
        elif file_names[i] == 'continent_data':
            df = transform_continent_data(df)

        # Push the transformed data to XCom
        kwargs['ti'].xcom_push(key=file_names[i],
                            value=df.to_json(orient='records'))
    return "Data transformed and pushed to XCom"
