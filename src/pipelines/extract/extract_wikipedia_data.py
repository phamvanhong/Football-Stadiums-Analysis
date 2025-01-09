import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.etl import ETL


def extract_wikipedia_data(**kwargs):
    """
    Extracts data sources from the wikipedia
    """
    # Setup variables
    urls = kwargs['urls']
    target_table_indexes = kwargs['target_table_indexes']
    file_names = kwargs['file_names']
    for i in range(len(urls)):
        url = urls[i]
        target_table_index = target_table_indexes[i]
        file_name = file_names[i]
    
        # ETL process
        etl = ETL(url)  # Tạo đối tượng ETL với URL hiện tại
        tables = etl.extract_()  # Trích xuất tất cả các bảng từ URL
    
        json_target_table = tables[target_table_index].to_json(orient='records')
        kwargs['ti'].xcom_push(key=f'{file_name}', value=json_target_table)
    return "Data extracted and pushed to XCom"


