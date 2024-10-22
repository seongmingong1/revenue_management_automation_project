import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd 



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