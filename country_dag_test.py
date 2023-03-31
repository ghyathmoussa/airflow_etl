from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import mysql.connector

# set the default arguments

default_args = {
    'owner':'airflow',
    'start_date': datetime(2023, 3, 24, 10, 0, 0),
    'retries': 1
}

dag = DAG(
    'country',
    default_args=default_args,
    schedule = timedelta(minutes=5)
)

# load currency from json file
def load_currency():
    with open('./currency.json','r') as f:
        data = json.load(f)
    return data

# load country from json file
def load_country():
    with open('./names.json','r') as f:
        data = json.load(f)
    return data

# save data to sql workbranch
def save_currency_to_sql():
    data = load_currency()
    conn = mysql.connector.connect(
        user='root',
        password = 'ghrefd11!',
        host = 'localhost',
        database='kartca_db'
    )

    cursor = conn.cursor()

    for country_code,currency_code in data.items():
        query = f'insert into currency (country_code,currency_code) values({country_code}, {currency_code})'
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def save_country_to_sql():
    data = load_country()
    conn = mysql.connector.connect(
        user='root',
        password = 'ghrefd11!',
        host = 'localhost',
        database='kartca_db'
    )

    cursor = conn.cursor()

    for country_code,country_name in data.items():
        
        query = f'insert into country (country_code,country_name) values({country_code}, {country_name})'

        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

# callable function
def start():
    print('starting DAG....')

def end():
    print('DAG is complete!')


t1 = PythonOperator(
    task_id='start_dag',
    python_callable=start,
    dag=dag
)

t2 = PythonOperator(
    task_id='read_country_names',
    python_callable=load_country,
    dag=dag
)
t3 = PythonOperator(
    task_id='write_country_names',
    python_callable=save_country_to_sql,
    dag=dag
)
t4 = PythonOperator(
    task_id='end_dag',
    python_callable=end,
    dag=dag
)