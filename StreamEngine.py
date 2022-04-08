# pip3 install watchdog
import watchdog.events
import watchdog.observers
import time
import csv
import XMLparser
from queue import Queue
import mysql.connector as mysql
from mysql.connector import Error


class ETL:
    def __init__(self):
        self.configFname = "config_v2.xml"
        self.parser = XMLparser.XMLParser(self.configFname)
        self.conn, self.cursor = self.connectToDb()
        self.insertionQ=Queue(maxsize=0)
        self.deletionQ=Queue(maxsize=0)
        self.tick=0
        self.windowParams=self.parser.getWindowparams()
        self.wsize=self.windowParams["window_size"]
        self.wvel=self.windowParams["window_velocity"]

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

    def extract(self, paths):
        print("In extract")
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
        print("In load")
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

    def delete(self,lines,pk):
        print("In delete")
        for line in lines:
            for index, item in enumerate(pk):
                sql = "DELETE FROM factTable WHERE "
                sql += item
                sql += "="
                sql += line[index]
            print(sql)
            try:
                (self.cursor).execute(sql)
                print("Record deleted")
                (self.conn).commit()
            except Error as e:
                print("Error while deleting from the database", e)

    def process(self, paths):
        print("In process")
        for path in paths:
            if self.tick==0:
                self.insertionQ.put(path)
                if self.insertionQ.qsize()==self.wsize:
                    lst_toinsert_paths = []
                    while self.insertionQ.qempty()==False:
                        temp_path = self.insertionQ.get()
                        (self.deletionQ).put(temp_path)
                        lst_toinsert_paths.append(temp_path)
                    self.load(self.extract(lst_toinsert_paths))
                    # ping aravind function to update/create views
                    tick=tick+1
            else:
                self.insertionQ.put(path)
                if self.insertionQ.qsize()==self.wvel:
                    lst_toinsert_paths = []
                    lst_todel_paths = []
                    while self.insertionQ.qempty==False:
                        tmp_deletion_path = self.deletionQ.get()
                        tmp_insertion_path = self.insertionQ.get()
                        self.deletionQ.put(tmp_insertion_path)
                        lst_toinsert_paths.append(tmp_insertion_path)
                        lst_todel_paths.append(tmp_deletion_path)
                    self.delete(self.extract(lst_todel_paths),self.parser.getPKfactTable())
                    self.load(self.extract(lst_toinsert_paths))
                    # call aravind function to create/update views
                    tick=tick+1
        # lines = self.extract(paths)
        # self.load(lines)


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        self.etl = ETL()
        # Set the patterns for PatternMatchingEventHandler
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv'],
                                                             ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        print(event.src_path)
        (self.etl).process([event.src_path])


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
