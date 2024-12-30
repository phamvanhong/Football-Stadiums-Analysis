from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def check_variable(**kwargs):
    # Lấy giá trị từ Variable
    azure_key = Variable.get("azure_storage_key")
    print(f"Azure Key: {azure_key[:5]}...")  # In một phần để kiểm tra
    
    # Đẩy giá trị lên XCom
    return azure_key

with DAG(
    dag_id="test_variable_check",
    start_date=datetime(2024, 12, 28),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="check_variable_task",
        python_callable=check_variable,
    )
