from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from  airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_merge',
    default_args=default_args,
    description='A DAG to merge country and currency data and write to a new table',
    schedule_interval=timedelta(hours=1),
)

start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "Data merge process started"',
    dag=dag,
)

merge_data_task = MySqlOperator(
    task_id='merge_data_task',
    mysql_conn_id='mysql_conn',
    sql='INSERT INTO data_merge SELECT * FROM country JOIN currency ON country.country_code = currency.country_code',
    dag=dag,
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "Data merge process completed"',
    dag=dag,
)

start_task >> merge_data_task >> end_task