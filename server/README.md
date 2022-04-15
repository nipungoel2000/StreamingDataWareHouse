# StreamingDataWareHouse

Required Installations:
pip3 install mysql-connector-python
pip3 install watchdog

Steps to run:
1. First execute the StreamEngine.py file which is responsible for watching the folder for any new data and extracting and loading it into the factTable (for now)
2. Then start the emitter.py file. This file generates a datapoint .csv file every second and dumps it into the factDataEmitter folder
