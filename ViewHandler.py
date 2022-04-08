# TO DO optimize delete buffer data calls for only base cuboid
from gettext import find
from ntpath import join
from select import select
from shlex import split
import mysql.connector
import XMLparser
from itertools import product


class ViewHandler():
    def __init__(self, xml_location, db_details):
        self.db = mysql.connector.connect(**db_details)
        self.cursor = None
        if self.db.is_connected():
            self.cursor = self.db.cursor()    

        self.XMLParser = XMLparser.XMLParser(xml_location)
        self.entryPoints, self.aggregates = self.getXMLData()
        self.tableColumns = self.getTableColumns()
        self.ftColumns, self.factVariables = self.getFTColumns()

        
    def getTableColumns(self):
        self.cursor.execute("show tables;")
        tableNames = self.cursor.fetchall()

        tableColumns = {}
        # Get dimension names & column information
        dimensionsNames = list(filter(lambda x: x[0][0:4] == 'dim_', tableNames))
        for dim in dimensionsNames:
            self.cursor.execute(f"show columns from {dim[0]}")
            dimColumns = self.cursor.fetchall()
            tableColumns[dim[0]] = dimColumns
        # Add dimensions entry point information
        for k, v in tableColumns.items():
            columnDesc = [list(i) for i in v]
            for i in columnDesc:  # Entry point default false
                if ".".join([k, i[0]]) in self.entryPoints:
                    i.append(True)
                else:
                    i.append(False)

            tableColumns[k] = columnDesc
        return tableColumns
    
    def getXMLData(self):
        epDict = self.XMLParser.getEntryPoints()
        entryPoints = []
        for k, v in epDict.items():
            entryPoints.extend(['dim_' + k + '.' + i for i in v])
        entryPoints.sort()
        aggregates = self.XMLParser.getAggregates()
        return (entryPoints, aggregates)

    def getFTColumns(self):
        # Get fact table column information
        self.cursor.execute("show columns from factTable")
        factTableColumns = self.cursor.fetchall()
        # Assuming all the dimension fks are also pks
        return [list(filter(lambda x: x[3] == 'PRI', factTableColumns)), list(filter(lambda x: x[3] != 'PRI', factTableColumns))]

    # Identify entrypoint element given a bitcode
    def decodeEntryPoints(self, entryPoints, bitcode):
        return [entryPoints[i] for i, e in enumerate(bitcode) if bitcode[i] == '1']

    # Create the lattice of cuboids
    def createViews(self):
        numEntryPoints = len(self.entryPoints)
        bitcodes = ["1"*numEntryPoints]

        # Generating bitcodes to uniquely represent elements in the power set of entry points
        self.generateBitcodes(bitcodes, '', numEntryPoints)

        # Create the lattice of cuboids
        for i, bitcode in enumerate(bitcodes):
            self.createCuboid(bitcode)
            self.cursor.fetchall()
            self.cursor.nextset()

    # Generate all binary combiations of strings of given length
    def generateBitcodes(self, bitcodes, bitcode, k):
        if (k == 0):
            bitcodes.append(bitcode)
            return
        newBitcode = bitcode + '0'
        self.generateBitcodes(bitcodes, newBitcode, k - 1)

        newBitcode = bitcode + '1'
        self.generateBitcodes(bitcodes, newBitcode, k - 1)

    def deleteBufferData(self):
        self.cursor.execute("truncate bufferFactTable");
        self.cursor.fetchall();
        self.db.commit();

    def createCuboid(self, tableCode):

        tableName = "mv" + tableCode

        # Base cuboid
        if tableCode.find('0') == -1:
            joinQuery = ""
            for i, (dimName, dimColumns) in enumerate(self.tableColumns.items()):
                pkName = [col[0] for col in dimColumns if col[3] == 'PRI'][0]
                singleJoinQuery = f"inner join {dimName} on f.{pkName} = {dimName}.{pkName} "
                joinQuery = joinQuery + singleJoinQuery

            viewEntryPoints = self.decodeEntryPoints(self.entryPoints, tableCode)
            entryPointColumns = (
                list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))

            fvColumns = list(map(lambda fv: f"f.{fv[0]}", self.factVariables))
            selectColumns = list(map(lambda t: " as ".join(t), list(
                zip(viewEntryPoints, entryPointColumns)))) + fvColumns

            # Drop if exists
            dropQuery = f"drop table if exists {tableName};"
            self.cursor.execute(dropQuery)
            self.cursor.fetchall()

            cuboidQuery = f"create table {tableName} ( select {', '.join(selectColumns)} from factTable f " + joinQuery + ");"
            
            self.cursor.execute(cuboidQuery)

        # Non base cuboid
        else:

            viewEntryPoints = self.decodeEntryPoints(self.entryPoints, tableCode)
            selectColumns = (
                list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))
            fvColumns = list(map(lambda fv: f"{fv[0]}", self.factVariables))

            aggregationQuery = list(map(lambda x: f"{x[0]}({x[1]}) {x[0]}_{x[1]}", list(
                product(self.aggregates, fvColumns))))

            # For apex cuboid
            isGroupBy = "group by " if len(selectColumns) else ""

            # Drop if exists
            dropQuery = f"drop table if exists {tableName};"
            self.cursor.execute(dropQuery)
            self.cursor.fetchall()
            cuboidQuery = f"create table {tableName} ( select {', '.join(selectColumns + aggregationQuery)} from mv{'1'*len(tableCode)} {isGroupBy} {', '.join(selectColumns)});"

            self.cursor.execute(cuboidQuery)

        self.cursor.fetchall()
        
        tickQuery = f" alter table {tableName} add tick int;"
        self.cursor.execute(tickQuery)
        self.cursor.fetchall()

    def updateCuboid(self, tableCode, tick):

        tableName = "mv" + tableCode

        # Base cuboid
        if tableCode.find('0') == -1:
            joinQuery = ""
            for i, (dimName, dimColumns) in enumerate(self.tableColumns.items()):
                pkName = [col[0] for col in dimColumns if col[3] == 'PRI'][0]
                singleJoinQuery = f"inner join {dimName} on f.{pkName} = {dimName}.{pkName} "
                joinQuery = joinQuery + singleJoinQuery

            viewEntryPoints = self.decodeEntryPoints(self.entryPoints, tableCode)
            entryPointColumns = (
                list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))

            fvColumns = list(map(lambda fv: f"f.{fv[0]}", self.factVariables))
            selectColumns = list(map(lambda t: " as ".join(t), list(
                zip(viewEntryPoints, entryPointColumns)))) + fvColumns

            cuboidQuery = None
            cuboidQuery = f"insert into {tableName} select {', '.join(selectColumns + ['f.tick'])} from bufferFactTable f " + joinQuery + ";"
                        
            self.cursor.execute(cuboidQuery)

        # Non base cuboid
        else:

            viewEntryPoints = self.decodeEntryPoints(self.entryPoints, tableCode)
            selectColumns = (
                list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))
            fvColumns = list(map(lambda fv: f"{fv[0]}", self.factVariables))

            aggregationQuery = list(map(lambda x: f"{x[0]}({x[1]}) {x[0]}_{x[1]}", list(
                product(self.aggregates, fvColumns))))

            # For apex cuboid
            isGroupBy = "group by " if len(selectColumns) else ""

            cuboidQuery = f"insert into {tableName} select {', '.join(selectColumns + aggregationQuery + ['tick'])} from mv{'1'*len(tableCode)} where tick = {tick} {isGroupBy} {', '.join(selectColumns)};"
            
            self.cursor.execute(cuboidQuery)

        self.cursor.fetchall()        
        self.db.commit()


    def createBufferFactTable(self):
        self.cursor.execute("create table bufferFactTable as select * from factTable;")
        self.cursor.fetchall()

        tickQuery = f" alter table bufferFactTable add tick int;"
        self.cursor.execute(tickQuery)
        self.cursor.fetchall()

    def updateViews(self, tick):
        ftColumns = [name[0] for name in (self.ftColumns + self.factVariables)]
        self.cursor.execute(f"insert into bufferFactTable ({', '.join(ftColumns)}, tick) select {', '.join(ftColumns)}, {tick} from factTable;")
        self.cursor.fetchall()
        self.db.commit()
        
        numEntryPoints = len(self.entryPoints)
        bitcodes = []

        # Generating bitcodes to uniquely represent elements in the power set of entry points
        self.generateBitcodes(bitcodes, '', numEntryPoints)
        bitcodes.sort()
        bitcodes = bitcodes[::-1]
        for bitcode in bitcodes:
            self.updateCuboid(bitcode, tick)
        
        self.deleteBufferData()

if __name__ == "__main__":
    viewHandler = ViewHandler("config_v2.xml", {
            "host": "localhost",
            "user": "root",
            "passwd": "sk@7NFJz",
            "database": "stdwh_db",
            "charset": 'utf8',
    })