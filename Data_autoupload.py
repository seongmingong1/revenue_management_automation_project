import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd 
import numpy as np 
from dotenv import load_dotenv
from forex_python.converter import CurrencyRates
import requests

load_dotenv()

def upload_to_mysql():

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("graceful-byway-438720-f4-96575e4be360.json", scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key("1tCJA672lncxgUAzZkdmfU1az9yqaPXK46707Wtj3N4o")
    worksheet = spreadsheet.get_worksheet(0)

    data = worksheet.get_all_values()

    #for row in data:
    #    print(row)

#Mysql connection 

    password = os.getenv("MYSQL_PASSWORD")

    conn = mysql.connector.connect(
        host = "localhost",
        user= "root",
        password = password,
        database="job_project"
)

    cursor = conn.cursor()

    try: 

        for row in data[1:]:
            lesson_date = datetime.strptime(row[1], "%Y.%m.%d")
            query = "INSERT IGNORE INTO lesson (student_id, lesson_date, lesson_type, student_name) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (row[0], lesson_date, row[2], row[3]))
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()


def calculate_daily_revenue(ti):
    password = os.getenv("MYSQL_PASSWORD")
    conn = mysql.connector.connect(
        host="localhost",
        user ="root",
        password=password,
        database= "job_project"
    )

    students_df = pd.read_sql("SELECT * FROM students", conn)
    lessons_df = pd.read_sql("SELECT * FROM lesson", conn)
    conn.close()

    merged_df = pd.merge(lessons_df, students_df, on ='student_id')
    merged_df['lesson_date'] = pd.to_datetime(merged_df['lesson_date'], errors='coerce')

    api_key = os.getenv("api_key")
    url = f"http://data.fixer.io/api/latest?access_key={api_key}"
    response = requests.get(url)
    data = response.json()  

    eur_to_usd = data['rates']['USD']  # 1 EUR -> USD 환율
    eur_to_rub = data['rates']['RUB']  # 1 EUR -> RUB
    eur_to_krw = data['rates']['KRW'] # 1EUR -> KRW
    EUR = data['rates']['EUR']

    usd_to_eur = 1 / eur_to_usd
    rub_to_eur = 1 / eur_to_rub
    usd_to_krw = usd_to_eur * eur_to_krw
    rub_to_krw = rub_to_eur * eur_to_krw


    #daily revenue
    daily_revenue = merged_df.groupby(['lesson_date','currency']).apply(lambda x: (x['fee']).sum()).unstack()
    daily_revenue['total_eur'] = (daily_revenue['USD'].fillna(0) * usd_to_eur + daily_revenue['RUB'].fillna(0) * rub_to_eur).round(2)
    daily_revenue['total_krw'] = (daily_revenue['KRW'].fillna(0) + daily_revenue['USD'].fillna(0) * usd_to_krw + daily_revenue['RUB'].fillna(0) * rub_to_krw).round(2)
    
    ti.xcom_push(key='daily_revenue', value=daily_revenue)



def calculate_weekly_revenue(ti):
    daily_revenue= ti.xcom_pull(key='daily_revenue', task_ids='task_2')
    daily_revenue = daily_revenue.reset_index()
    daily_revenue['lesson_date'] = pd.to_datetime(daily_revenue['lesson_date'])

    weekly_revenue = daily_revenue.groupby(pd.Grouper(key='lesson_date', freq='W-SUN')).sum()

    ti.xcom_push(key='weekly_revenue', value=weekly_revenue)

def calculate_montly_revenue(ti):
    daily_revenue= ti.xcom_pull(key='daily_revenue', task_ids='task_2')
    daily_revenue = daily_revenue.reset_index()
    daily_revenue['lesson_date'] = pd.to_datetime(daily_revenue['lesson_date'])

    # 월별 수입 계산 (이번 달도 포함)
    current_date = pd.to_datetime('today')

    monthly_revenue = daily_revenue[daily_revenue['lesson_date'] <= current_date].groupby(
    pd.Grouper(key='lesson_date', freq='M')).sum()

    ti.xcom_push(key='monthly_revenue', value=monthly_revenue)

def save_to_csv(ti):
    daily_revenue = ti.xcom_pull(key='daily_revenue', task_ids='task_2')
    weekly_revenue = ti.xcom_pull(key='weekly_revenue', task_ids='task_3')
    monthly_revenue = ti.xcom_pull(key='monthly_revenue', task_ids='task_4')

    daily_revenue.to_csv('daily_revenue.csv', index=False)
    weekly_revenue.to_csv('weekly_revenue.csv', index=False)
    weekly_revenue.to_csv('monthly_revenue.csv', index=False)

    print("CSV files saved successfully.")

with DAG(
    dag_id = "Data_autouploaded",
    schedule_interval= "0 0 * * *",
    start_date=pendulum.datetime(2024,10,1, tz="Europe/Madrid"),
    catchup= False
) as dag: 
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable=upload_to_mysql

    )

    task_2 =PythonOperator(
        task_id = 'task_2',
        python_callable=calculate_daily_revenue
    )

    task_3 = PythonOperator(
        task_id = 'task_3',
        python_callable=calculate_weekly_revenue
    )

    task_4 = PythonOperator(
        task_id = 'task_4',
        python_callable=calculate_montly_revenue
    )

    task_5 = PythonOperator(
        task_id ='task_5',
        python_callable=save_to_csv
    )

task_1 >> task_2 >> task_3 >> task_4 >> task_5




