from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import mysql.connector


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 27, 10, 5),
    'retries': 1
}

dag = DAG(
    'currency_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# main functions

def start_dag():
    print("currency DAG started...")

def end_dag():
    print("currency DAG finished...")

def load_currency():
    with open('../currency.json','r') as f:
        data = json.load(f)
    return data

def save_to_sql():
    data = load_currency()
    conn = mysql.connector.connect(
        user='kartca',
        password='kartca',
        host='localhost',
        database='kartca_db',
    )
    cursor = conn.cursor()
    for country_code,currency_code in data.items():
        query = f'insert into currency (country_code,currency_code) values({country_code}, {currency_code})'
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

start_task = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=dag
)

read_task = PythonOperator(
    task_id='read_json',
    python_callable=load_currency,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_data',
    python_callable=save_to_sql,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=dag
)

start_task >> read_task >> insert_task >> end_task