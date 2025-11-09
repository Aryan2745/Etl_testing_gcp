from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Simple Python function
def say_hello():
    print("=" * 50)
    print("🎉 Hello from Airflow!")
    print("✅ Your DAG deployment is working!")
    print("=" * 50)
    return "Success!"

def print_date():
    print(f"Current date and time: {datetime.now()}")
    print("This is running inside Composer!")

# Define the DAG
with DAG(
    dag_id='hello_world_test',
    default_args=default_args,
    description='Simple test DAG with print statements',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'hello'],
) as dag:

    # Task 1: Print hello message
    task1 = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )
    
    # Task 2: Print current date
    task2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )
    
    # Task 3: Simple bash command
    task3 = BashOperator(
        task_id='print_bash',
        bash_command='echo "Hello from Bash! Deployment successful!"',
    )
    
    # Set task order
    task1 >> task2 >> task3
