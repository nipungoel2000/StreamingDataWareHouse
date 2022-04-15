#!/bin/bash

javac ConfigParser.java
java -classpath "/usr/share/java/mysql-connector-java-8.0.28.jar:." ConfigParser
python3 createViews.py

python3 StreamEngine.py & python3 emitter.py