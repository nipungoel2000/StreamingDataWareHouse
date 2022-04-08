# pip3 install watchdog
import watchdog.events
import watchdog.observers
import time
import csv
import module
import mysql.connector as mysql
from mysql.connector import Error

class ETL:
    def __init__(self):
        self.configFname = "config_v2.xml"
        self.parser = module.XMLParser(configFname)
        conn, cursor = self.connectToDb()

    def connectToDb(self):
        # configure your database credentials here or we can even send them as paramters in the function
        host = 'localhost'
        database = 'stdwh_db'
        user = 'root'
        password = 'root'
        try:
            conn = mysql.connect(user=user, password=password,
                                 host=host, database=database)
            if conn.is_connected():
                cursor = conn.cursor()
        except Error as e:
            print("Error while connecting to MySQL", e)
        return [conn, cursor]


    def extract(self,paths):
        lines = []
        for csv_path in paths:
            with open(csv_path, mode='r')as file:
                csvFile = csv.reader(file)
                for line in csvFile:
                    lines.append(line)

        # printing the lines read from csv for testing
        for line in lines:
            print(line)
        return lines

    def load(self, lines):
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
                (self.cursor).execute(sql)
                print("Record inserted")
                (self.conn).commit()
            except Error as e:
                print("Error while inserting to the database", e)


    def process(self, paths):
        lines = self.extract(paths) 
        self.load(lines)


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        etl = ETL()
        # Set the patterns for PatternMatchingEventHandler
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv'],
                                                             ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        print(event.src_path)
        etl.process([event.src_path])


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
