
import logging
# logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lesson_etl.log'),
        logging.StreamHandler()
    ]

import os
from config import Config
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pandas as pd 
import numpy as np 
from dotenv import load_dotenv
from forex_python.converter import CurrencyRates
import requests
import pymysql


pymysql.install_as_MySQLdb()
load_dotenv()

file_path = Variable.get("lesson_data_path", default_var="/Users/miniggon/Desktop/project/lesson_data")
Config.update_file_path(file_path)


"""def connect_to_db():
    try: 
        password = os.getenv("MYSQL_PASSWORD")
        connection = mysql.connector.connect(
            host = "localhost",
            user= "root",
            password = password,
            database="job_project"

        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None
"""

# Database connection function 

def connect_to_db():
    #connection = None
    try:
        hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = hook.get_conn()
        return connection
    
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise
    

def get_latest_file(folder_path):
    try:
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
        if not files:
            logging.error("No excel files found in directory")
            return None
        latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(folder_path, f)))
        return os.path.join(folder_path, latest_file)
    except Exception as e:
        logging.error(f"No excel files found in directory: {e}")
        raise
        


def upload_mysql(file_path):
    connection = connect_to_db()
    if connection is None: 
        logging.error("Database connection failed")
        return
    
    cursor = None
    try: 
        df = pd.read_excel(file_path)
        validate_lesson_data(df)

        cursor = connection.cursor()

        for _, row in df.iterrows():
            lesson_date = pd.to_datetime(row['lesson_date']).strftime('%Y-%m-%d')
            insert_query = """
            INSERT IGNORE INTO lesson (student_id, lesson_date, lesson_type, student_name)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['student_id'],
                lesson_date,
                row['lesson_type'],
                row['student_name']
                ))
    
        connection.commit()
        logging.info(f"Data from {file_path} successfully inserted into MySQL")
    except Exception as e:
        logging.error(f"Error in upload_mysql: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def validate_lesson_data(df): 
    required_columns = ['student_id', 'lesson_date', 'lesson_type','student_name' ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns: 
        raise ValueError(f"requeired columns have been missed: {missing_columns}")
    
    if not pd.to_datetime(df['lesson_date'], errors='coerce').notna().all():
        raise ValueError("invaild data format in 'lesson_date'")
    
    if df['student_id'].isnull().any():
        raise ValueError("Null Value in student_id")

def get_exchange_rates(): 
    try: 
        api_key = os.getenv("api_key")
        url = f"http://data.fixer.io/api/latest?access_key={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()  

        if not data.get('success', False):
            raise ValueError(f"API failed: {data.get('error','unknown')}")
        
        # currency calculation 
        eur_to_usd = data['rates']['USD']  # 1 EUR -> USD 환율
        eur_to_rub = data['rates']['RUB']  # 1 EUR -> RUB
        eur_to_krw = data['rates']['KRW'] # 1EUR -> KRW
        EUR = data['rates']['EUR']

        usd_to_eur = 1 / eur_to_usd
        rub_to_eur = 1 / eur_to_rub
        usd_to_krw = usd_to_eur * eur_to_krw
        rub_to_krw = rub_to_eur * eur_to_krw

        return {
            'usd_to_eur': usd_to_eur,
            'rub_to_eur': rub_to_eur,
            'eur_to_krw': eur_to_krw,
            'usd_to_krw': usd_to_krw,
            'rub_to_krw': rub_to_krw
        }
    except requests.Timeout:
        logging.error("currency rate API request Time out")
        raise

    except requests.RequestException as e :
        logging.error(f"currency API request failed: {e}")
        raise

def calculate_daily_revenue(ti):
    try:
        logging.info("Daily revenue calculation started")

        hook = MySqlHook(mysql_conn_id = 'airflow_db')
        conn = hook.get_conn()


        students_df = pd.read_sql("SELECT * FROM students", conn)
        lessons_df = pd.read_sql("SELECT * FROM lesson", conn)
        conn.close()

        merged_df = pd.merge(lessons_df, students_df, on ='student_id')
        merged_df['lesson_date'] = pd.to_datetime(merged_df['lesson_date'], errors='coerce')

        rates = get_exchange_rates()


        #daily revenue
        daily_revenue = merged_df.groupby(['lesson_date','currency']).apply(lambda x: (x['fee']).sum()).unstack()
        daily_revenue['total_eur'] = (
            daily_revenue['USD'].fillna(0) * rates['usd_to_eur'] +
            daily_revenue['RUB'].fillna(0) * rates['rub_to_eur'] + 
            daily_revenue['EUR'].fillna(0)
            ).round(2)
    
        daily_revenue['total_krw'] = (
            daily_revenue['KRW'].fillna(0) + 
            daily_revenue['USD'].fillna(0) * rates['usd_to_krw'] + 
            daily_revenue['RUB'].fillna(0) * rates['rub_to_krw'] + 
            daily_revenue['EUR'].fillna(0)* rates['eur_to_krw']
            ).round(2)
    
        daily_revenue.to_csv(f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv", index = False)
        ti.xcom_push(key='daily_revenue_path', value=f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv")

    except Exception as e :
        logging.error(f"Error in daily revenue calculation: {e}")
        raise




def calculate_weekly_revenue(ti):

    try: 
        logging.info("Starting weekly revenue calculation")
   
        daily_revenue_path = ti.xcom_pull(key='daily_revenue_path', task_ids='calculate_daily_revenue')
        daily_revenue = pd.read_csv(daily_revenue_path)
        daily_revenue['lesson_date'] = pd.to_datetime(daily_revenue['lesson_date'])

        weekly_revenue = daily_revenue.groupby(pd.Grouper(key='lesson_date', freq='W-SUN')).sum()


        weekly_revenue_path = f"{Config.CSV_OUTPUT_PATH}weekly_revenue.csv"
        weekly_revenue.to_csv(weekly_revenue_path, index=False)
        ti.xcom_push(key='weekly_revenue_path', value=f"{Config.CSV_OUTPUT_PATH}weekly_revenue.csv")

        logging.info("weekly revenue calculation completed")

    except Exception as e:
        logging.error(f"Error in weekly revenue calculation: {e}")
        raise



def calculate_monthly_revenue(ti):

    try:
    
        logging.info("Starting monthly revenue calculation")
    
        daily_revenue= ti.xcom_pull(key='daily_revenue', task_ids='calculate_daily_revenue')
        daily_revenue = daily_revenue.reset_index()
        daily_revenue['lesson_date'] = pd.to_datetime(daily_revenue['lesson_date'])

        # 월별 수입 계산 (이번 달도 포함)
        current_date = pd.to_datetime('today')

        monthly_revenue = daily_revenue[daily_revenue['lesson_date'] <= current_date].groupby(
        pd.Grouper(key='lesson_date', freq='M')).sum()

        ti.xcom_push(key='monthly_revenue', value=monthly_revenue)

        logging.info("monthly revenue calculation completed")

    except Exception as e:
        logging.error(f"Error in monthly revenue calculation: {e}")
        raise
        
def save_to_csv(ti):

    try:
        logging.info("Starting to save CSV files")
    
        daily_revenue = ti.xcom_pull(key='daily_revenue', task_ids='calculate_daily_revenue')
        weekly_revenue = ti.xcom_pull(key='weekly_revenue', task_ids='calculate_weekly_revenue')
        monthly_revenue = ti.xcom_pull(key='monthly_revenue', task_ids='calculate_monthly_revenue')

        daily_revenue.to_csv(f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv", index=False)
        weekly_revenue.to_csv(f"{Config.CSV_OUTPUT_PATH}weekly_revenue.csv", index=False)
        monthly_revenue.to_csv(f"{Config.CSV_OUTPUT_PATH}monthly_revenue.csv", index=False)

        logging.info("CSV files saved successfully.")

    except Exception as e: 
        logging.error(f"Error saving CSV files: {e}")
        raise

with DAG(
    dag_id = "Data_autouploaded",
    schedule = "0 0 * * *",
    start_date=pendulum.datetime(2024,10,1, tz="Europe/Madrid"),
    catchup= False
) as dag: 
    upload_task = PythonOperator(
        task_id = 'upload_task',
        python_callable=upload_mysql,
        op_args=[os.path.join(os.path.dirname(__file__), 'lesson_data')]

    )

    daily_task =PythonOperator(
        task_id = 'daily_task',
        python_callable=calculate_daily_revenue
    )

    weekly_task = PythonOperator(
        task_id = 'weekly_task',
        python_callable=calculate_weekly_revenue
    )

    monthly_task = PythonOperator(
        task_id = 'monthly_task',
        python_callable=calculate_monthly_revenue
    )

    csv_task = PythonOperator(
        task_id ='csv_task',
        python_callable=save_to_csv
    )

upload_task >> daily_task >> weekly_task >> monthly_task >> csv_task
