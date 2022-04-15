import mysql.connector as mysql
from mysql.connector import Error

db_details = {"host": "localhost",
              "user": "root",
              "passwd": "root",
              "database": "stdwh_db",
              "charset": 'utf8'}
try:
    conn = mysql.connect(**db_details)
    if conn.is_connected():
        cursor = conn.cursor()
        try:
            cursor.execute('DROP DATABASE `stdwh_db`')
            conn.commit()
            print("dropped the database")
        except Error as e:
            print("Error while dropping the database", e)
except Error as e:
    print("Error while connecting to MySQL", e)
