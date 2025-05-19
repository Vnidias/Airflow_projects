from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import random
import pendulum

def generate_random_number(**context):
    ti = context['ti']
    number = random.randint(1, 100)
    ti.xcom_push(key='random_number', value=number)
    print(f"Generated random number: {number}")

def check_even_odd(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    result = "even" if number % 2 == 0 else "odd"
    print(f"The number {number} is {result}.")

with DAG(
    'random_number_checker',
    start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone("UTC")),
    end_date=datetime(2025, 1, 1, tzinfo=pendulum.timezone("UTC")),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False,
    doc_md="""
    # Random Number Checker DAG

    This DAG generates a random number and checks if it's even or odd.
    """
) as dag:

    generate_task = PythonOperator(
        task_id='generate_number',
        python_callable=generate_random_number,
    )

    check_task = PythonOperator(
        task_id='check_even_odd',
        python_callable=check_even_odd,
    )

    generate_task >> check_task