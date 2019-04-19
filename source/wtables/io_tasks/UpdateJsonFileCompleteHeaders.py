import bz2
import os
import traceback
import sys
import numpy as np

from wtables.schema.Article import *
from wtables.preprocessing import ReadHTML as readHTML
from wtables.preprocessing.TextProcessing import TextProcessing
import wtables.utils.Pipey as Pipey
import time
import logging
from wtables.wikidata_db.ConfigProperties import ConfigProperties
import re


def writeTable(table2d, tableId):
    ft = open(os.path.join(FOLDER_TABLES_OUT, str(tableId.replace(".", "_")) + ".json"), "w")
    ft.write(json.dumps(table2d.reprJSON(), cls=ComplexEncoder, skipkeys=True))
    ft.close()


def updateJsonFile(fileName):
    print("filename: ", fileName)
    tableId = fileName.split("/")[len(fileName.split("/")) - 1].replace(".json", "").replace("_", ".")
    file = open(fileName, "r")
    obj = file.read()
    obj = json.loads(obj)
    # Converting json to Table object
    table = ComplexDecoderTable().default(obj)

    if table.tableType == None or table.tableType.value == "":
        table.setColHeaders([])
        table.setStartRows(0)
        writeTable(table, tableId)
        return
    if table.tableType.value != TableType.WELL_FORMED.value:
        table.setTableType(table.tableType.value)
        table.setColHeaders([])
        table.setStartRows(0)
        writeTable(table, tableId)
    else:
        startRow = table.startRows
        if startRow == 0:
            table.setTableType(table.tableType.value)
            table.setColHeaders([])
            table.setStartRows(startRow)
            writeTable(table, tableId)
        else:
            table.setTableType(table.tableType.value)
            startRows, colHeadersType = readHTML.getColHeaderAllLevels(table.htmlMatrix, table.startRows,
                                                                   textProcessing)
            table.setColHeaders(colHeadersType)
            writeTable(table, tableId)




def readDocuments(input=0):
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    for file in files:
        name, file_extension = os.path.splitext(file)
        if file_extension == ".json":
            yield path + "/" + file
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", file)
    updateJsonFile(file)


if __name__ == '__main__':
    args = sys.argv[1:]
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJson"  # params.get("json_files")
    FOLDER_TABLES_OUT="/home/jluzuria/tablesJsonCompleteHeaders"
    textProcessing = TextProcessing()

    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)
    # One process combines the results into a file.
    pipeline.run(100)
