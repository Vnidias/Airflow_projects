from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Constants
OUTPUT_PATH = "/tmp/active_users.csv"

#Mock data
MOCK_DATA = [
    {"id": 1, "name": "Leanne Graham", "username": "Bret", "email": "Sincere@april.biz"},
    {"id": 2, "name": "Ervin Howell", "username": "Antonette", "email": "Shanna@melissa.tv"},
    {"id": 3, "name": "Clementine Bauch", "username": "Samantha", "email": "Nathan@yesenia.net"},
    {"id": 4, "name": "Patricia Lebsack", "username": "Karianne", "email": "Julianne.OConner@kory.org"},
    {"id": 5, "name": "Chelsey Dietrich", "username": "Kamren", "email": "Lucio_Hettinger@annie.ca"},
    {"id": 6, "name": "Mrs. Dennis Schulist", "username": "Leopoldo_Corkery", "email": "Karley_Dach@jasper.info"}
]


def extract_data(**kwargs):
    print("Using mocked user data")
    kwargs['ti'].xcom_push(key='raw_data', value=MOCK_DATA)


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data')

    if not raw_data:
        raise ValueError("No data received from extract_data task")

    df = pd.DataFrame(raw_data)

    if 'id' not in df.columns:
        raise ValueError("Expected 'id' column not found in the data")

    active_users = df[df['id'] <= 5]  # Sample transformation
    active_users.to_csv(OUTPUT_PATH, index=False)
    kwargs['ti'].xcom_push(key='output_file', value=OUTPUT_PATH)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='data_engineering_pipeline_offline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    #Correct way to define dependencies
    extract_task >> transform_task