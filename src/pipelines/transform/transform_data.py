import sys
sys.path.insert(0, '/opt/airflow/src/')
from common.constants import *
from objects.etl import ETL
import pandas as pd
import json


def transform_football_stadiums_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Additional transformation for the football_stadiums_data dataset

    Args:
        dataframe: pd.DataFrame - the dataframe to transform
    Returns:
        pd.DataFrame - the transformed
    """

    football_stadium_df = dataframe
    # Remove commas from the Capacity column
    football_stadium_df['capacity'] = football_stadium_df['capacity'].str.replace(
        ',', '', regex=True)
    
    # Fill missing values in the home_teams columns with "Unknown"
    football_stadium_df['home_teams'] = football_stadium_df['home_teams'].fillna("Unknown")

    # Change values in the Region column to "Asia" if they contain the word "Asia"
    football_stadium_df['continent'] = football_stadium_df['continent'].str.replace(
        r'.*Asia.*', 'Asia', regex=True)
    return football_stadium_df


def transform_continent_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Additional transformation for the continent_data dataset

    Args:
        dataframe: pd.DataFrame - the dataframe to transform
    Returns:
        pd.DataFrame - the transformed
    """
    continent_df = dataframe
    continent_df['continent_id'] = continent_df['continent_id'].fillna("NA")
    return continent_df


def transform_extracted_data(**kwargs) -> str:
    """
    Transform all datasets including basic transformation (drop columns, rename columns) and additional transformation for specific datasets

    Args:
        **kwargs - additional arguments
    Returns:
    """
    # Setup variables
    file_names = kwargs[FILE_NAMES]
    cols_drop = kwargs[COLS_DROP]
    cols_rename = kwargs[COLS_RENAME]
    etl = ETL(EMPTY_STRING)

    # Transform all datasets
    for i in range(len(file_names)):
        data = kwargs['ti'].xcom_pull(key=file_names[i],
                                      task_ids=EXTRACT_WIKIPEDIA_DATA,
                                      dag_id=ETL_FLOW,
                                      include_prior_dates=True)
        df = pd.DataFrame(json.loads(data))

        # Basic transformation for all datasets
        df = etl.transform(df,
                           cols_drop=cols_drop[i],
                           cols_rename=cols_rename[i])

        # Additional transformation for specific datasets
        if file_names[i] == FOOTBALL_STADIUMS:
            df = transform_football_stadiums_data(df)
        elif file_names[i] == CONTINENT:
            df = transform_continent_data(df)

        # Push the transformed data to XCom
        kwargs[TI].xcom_push(key=file_names[i],
                               value=df.to_json(orient=RECORDS))
    return "Data transformed and pushed to XCom"
