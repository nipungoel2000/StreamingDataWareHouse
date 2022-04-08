# Add dim in create script code
# In hierarchical dimension attrbutes, group by only one
# get table name hashes from sorted name arrangement
from gettext import find
from ntpath import join
from select import select
from shlex import split
import mysql.connector
import xml.etree.ElementTree as ET
from itertools import product


db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="stdwh_db",
    charset='utf8',
    # auth_plugin='mysql_native_password'
)


# Get entry points and aggregates from XML data
def getXMLData(xmlLocation):
    tree = ET.parse(xmlLocation)
    root = tree.getroot()

    entryPoints = []
    dimensions = root.find('dimensions')
    for dim in dimensions.findall('dim'):
        dimName = dim.attrib['name']
        entryPoints.extend(list(map(lambda x: ".".join([dimName, x.find('name').text]), list(
            filter(lambda x: x.attrib['is-EntryPoint'] == "true", dim.findall('field'))))))

    aggregateNodes = root.find('aggregates')
    aggregates = list(map(lambda agg: agg.text, aggregateNodes.findall('agg')))

    return entryPoints, aggregates


# Get the bitcode from given elements
def getTableCode(viewEntryPoints, allEntryPoints):
    bitcode = ""
    viewEntryPoints.sort()

    i_v = 0
    i_a = 0
    while i_a != len(allEntryPoints) and i_v != len(viewEntryPoints):
        if (viewEntryPoints[i_v] == allEntryPoints[i_a]):
            i_v += 1
            i_a += 1
            bitcode += '1'
        else:
            i_a += 1
            bitcode += '0'

    while i_a != len(allEntryPoints):
        bitcode += '0'
        i_a += 1

    return bitcode

# Identify entrypoint element given a bitcode


def decodeEntryPoints(entryPoints, bitcode):
    return [entryPoints[i] for i, e in enumerate(bitcode) if bitcode[i] == '1']

# Generate all binary combiations of strings of given length


def generateBitcodes(bitcodes, bitcode, k):
    if (k == 0):
        bitcodes.append(bitcode)
        return
    newBitcode = bitcode + '0'
    generateBitcodes(bitcodes, newBitcode, k - 1)

    newBitcode = bitcode + '1'
    generateBitcodes(bitcodes, newBitcode, k - 1)


def createCuboid(cursor, tableColumns, tableCode, entryPoints, factVariables, aggregates):

    # Base cuboid
    if bitcode.find('0') == -1:
        joinQuery = ""
        for i, (dimName, dimColumns) in enumerate(tableColumns.items()):
            pkName = [col[0] for col in dimColumns if col[3] == 'PRI'][0]
            singleJoinQuery = f"inner join {dimName} on f.{pkName} = {dimName}.{pkName} "
            joinQuery = joinQuery + singleJoinQuery

        viewEntryPoints = decodeEntryPoints(entryPoints, tableCode)
        entryPointColumns = (
            list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))

        fvColumns = list(map(lambda fv: f"f.{fv[0]}", factVariables))
        selectColumns = list(map(lambda t: " as ".join(t), list(
            zip(viewEntryPoints, entryPointColumns)))) + fvColumns

        # Drop if exists
        dropQuery = f"drop table if exists base_cuboid;"
        cursor.execute(dropQuery)
        cursor.fetchall()

        cuboidQuery = f"create table base_cuboid ( select {', '.join(selectColumns)} from factTable f " + joinQuery + ");"

        cursor.execute(cuboidQuery)

    # Non base cuboid
    else:
        tableName = "mv" + tableCode

        viewEntryPoints = decodeEntryPoints(entryPoints, tableCode)
        selectColumns = (
            list(map(lambda ep: "_".join(ep.split('.')), viewEntryPoints)))
        fvColumns = list(map(lambda fv: f"{fv[0]}", factVariables))

        aggregationQuery = list(map(lambda x: f"{x[0]}({x[1]}) {x[0]}_{x[1]}", list(
            product(aggregates, fvColumns))))

        # For apex cuboid
        isGroupBy = "group by " if len(selectColumns) else ""

        # Drop if exists
        dropQuery = f"drop table if exists {tableName};"
        cursor.execute(dropQuery)
        cursor.fetchall()

        cuboidQuery = f"create table {tableName} ( select {', '.join(selectColumns + aggregationQuery)} from base_cuboid {isGroupBy} {', '.join(selectColumns)});"

        cursor.execute(cuboidQuery)

        return 0


if db.is_connected():
    cursor = db.cursor()

    cursor.execute("show tables;")
    tableNames = cursor.fetchall()

    tableColumns = {}

    # Get dimension names & column information
    dimensionsNames = list(filter(lambda x: x[0][0:3] == 'dim', tableNames))

    entryPoints, aggregates = getXMLData("../config_v2.xml")
    entryPoints.sort()

    for dim in dimensionsNames:
        cursor.execute(f"show columns from {dim[0]}")
        dimColumns = cursor.fetchall()
        tableColumns[dim[0]] = dimColumns
    # Add dimensions entry point information
    for k, v in tableColumns.items():
        columnDesc = [list(i) for i in v]
        for i in columnDesc:  # Entry point default false
            if ".".join([k, i[0]]) in entryPoints:
                i.append(True)
            else:
                i.append(False)

        tableColumns[k] = columnDesc

    # Get fact table column information
    cursor.execute("show columns from factTable")
    factTableColumns = cursor.fetchall()
    # Assuming all the dimension fks are also pks
    factVariables = list(filter(lambda x: x[3] != 'PRI', factTableColumns))

    numEntryPoints = len(entryPoints)
    bitcodes = ["1"*numEntryPoints]

    # Generating bitcodes to uniquely represent elements in the power set of entry points
    generateBitcodes(bitcodes, '', numEntryPoints)

    # Create the lattice of cuboids
    for i, bitcode in enumerate(bitcodes):
        createCuboid(cursor, tableColumns, bitcode,
                     entryPoints, factVariables, aggregates)
        cursor.fetchall()
        cursor.nextset()
