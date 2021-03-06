# pip3 install watchdog
# TO DO create threads for updating views
import watchdog.events
import watchdog.observers
import time
import csv
import XMLparser
from queue import Queue
import mysql.connector as mysql
from mysql.connector import Error
from ViewHandler import ViewHandler
from pathlib import Path

class ETL:
    def __init__(self, db_details):
        self.configFname = "config_v2.xml"
        self.parser = XMLparser.XMLParser(self.configFname)
        self.conn, self.cursor = self.connectToDb(db_details)
        self.insertionQ = Queue()
        self.deletionQ = Queue()
        self.tick = 0
        self.windowParams = self.parser.getWindowparams()
        self.wsize = int(self.windowParams["window_size"])
        self.wvel = int(self.windowParams["window_velocity"])
        self.viewHandler = ViewHandler(self.configFname, db_details)

    def connectToDb(self, db_details):
        # configure your database credentials here or we can even send them as paramters in the function
        try:
            conn = mysql.connect(**db_details)
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

    def delete(self, lines, pk):
        print("In delete")
        for line in lines:
            sql = "DELETE FROM factTable WHERE "
            for index, item in enumerate(pk):
                sql += item
                sql += "="
                sql += line[index]
                sql += " and "
            sql = sql[:-5]
            print(sql)
            try:
                (self.cursor).execute(sql)
                print("Record deleted")
                (self.conn).commit()
            except Error as e:
                print("Error while deleting from the database", e)

    def process(self, paths):
        print("In process")
        print(paths)
        for path in paths:
            if self.tick == 0:
                self.insertionQ.put(path)
                print("path : " + path)
                print(self.wsize)
                print(self.insertionQ.qsize())
                if (self.insertionQ.qsize() == self.wsize):
                    print("here")
                    lst_toinsert_paths = []
                    while self.insertionQ.empty() == False:
                        temp_path = self.insertionQ.get()
                        (self.deletionQ).put(temp_path)
                        lst_toinsert_paths.append(temp_path)
                    self.load(self.extract(lst_toinsert_paths))
                    print("Executing update views")
                    self.viewHandler.updateViews(self.tick)
                    self.tick = self.tick+1
            else:
                self.insertionQ.put(path)
                if self.insertionQ.qsize() == self.wvel:
                    lst_toinsert_paths = []
                    lst_todel_paths = []
                    while self.insertionQ.empty() == False:
                        tmp_deletion_path = self.deletionQ.get()
                        tmp_insertion_path = self.insertionQ.get()
                        self.deletionQ.put(tmp_insertion_path)
                        lst_toinsert_paths.append(tmp_insertion_path)
                        lst_todel_paths.append(tmp_deletion_path)
                    self.delete(self.extract(lst_todel_paths),
                                self.parser.getPKfactTable())
                    self.load(self.extract(lst_toinsert_paths))
                    print("Executing update views")
                    self.viewHandler.updateViews(self.tick)
                    # call aravind function to create/update views
                    self.tick = self.tick+1
        # lines = self.extract(paths)
        # self.load(lines)


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        self.etl = ETL({
            "host": "localhost",
            "user": "root",
            "passwd": "root",
            "database": "stdwh_db",
            "charset": 'utf8',
        })
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
    observer.schedule(event_handler, path=src_path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    # curcount = 1
    # etl = ETL({
    #         "host": "localhost",
    #         "user": "root",
    #         "passwd": "root",
    #         "database": "stdwh_db",
    #         "charset": 'utf8',
    #     })
    # while(True):
    #     fname = str(curcount) + ".csv"
    #     path_to_file = src_path+fname
    #     path = Path(path_to_file)
    #     if path.is_file():
    #         print(path)
    #         etl.process([str(path)])
    #         curcount = curcount + 1
