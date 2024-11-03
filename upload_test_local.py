import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import pandas as pd

def connect_to_db():
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
    
def get_latest_file(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    if not files:
        print("No files")
        return None
    latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(folder_path, f)))
    return os.path.join(folder_path, latest_file)

def upload_mysql(file_path, connection):
    df =pd.read_excel(file_path)
    cursor = connection.cursor() #cursor

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
    print(f"Data form {file_path} inserted into MySQL table")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(base_dir, 'lesson_data')
    latest_file = get_latest_file(folder_path)
    
    if latest_file is None:
        print("No new file to upload")
        return

    connection = connect_to_db()

    if connection is None:
        print("Failed to connect to the db")
        return
    
    try: 
        upload_mysql(latest_file, connection)
    except Error as e:
        print(f"Error during upload: {e}")
    finally: 
        if connection.is_connected():
            connection.close()
            print("MYSQL Connection is closed")

main()
