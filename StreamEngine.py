#pip3 install watchdog
import watchdog.events
import watchdog.observers
import time
import csv
import mysql.connector as mysql
from mysql.connector import Error

def ETL(paths):
    #configure your database credentials here
    host='localhost'
    database='stdwh_db' 
    user='root'
    password='root'

    lines = []
    for csv_path in paths: 
        with open(csv_path, mode ='r')as file:
            csvFile = csv.reader(file)
            for line in csvFile:
                lines.append(line)
    
    # printing the lines read from csv for testing
    for line in lines:
        print(line)    

    try:
        conn = mysql.connect(user=user, password=password, host=host, database=database)
        if conn.is_connected():
            cursor = conn.cursor()
            for row in lines:
                sql = "INSERT INTO factTable VALUES ("
                for val in row:
                    sql += val
                    sql += ", "
                sql = sql[:-2]
                sql += ")"
                print(sql)
                # Handling duplicate entry error using try catch block
                try:
                    cursor.execute(sql)
                    print("Record inserted")
                    conn.commit()
                except Error as e:
                    print("Error while inserting to the database", e)
                
    except Error as e:
        print("Error while connecting to MySQL", e)

class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        # Set the patterns for PatternMatchingEventHandler
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv'],
															ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        print(event.src_path)
        ETL([event.src_path])

if __name__ == "__main__":
	src_path = r"fact_data_emitter/"
	event_handler = Handler()
	observer = watchdog.observers.Observer()
	observer.schedule(event_handler, path=src_path, recursive=True)
	observer.start()
	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()