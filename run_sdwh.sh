#!/bin/bash

javac ConfigParser.java
java -classpath "/usr/share/java/mysql-connector-java-8.0.22.jar:." ConfigParser
python createViews.py

python StreamEngine.py & python emitter.py