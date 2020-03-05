from wtables.schema.Article import *
import gzip
import sys
import os
import json
import numpy as np
import re

from wtables.utils import Pipey
from wtables.utils.ResultCombiner import *

def readTable(fileName):
    """
    Read table from json file
    :param fileName:
    :return:
    """
    try:
        file = open(os.path.join(FOLDER_JSON_FILES,fileName), "r")
        obj = file.read()
        obj = json.loads(obj)
        #Converting json to Table object
        table= ComplexDecoderTable().default(obj)
        return table
    except Exception as ex:
        print(ex)
        return None

def filterTables(fileName):
    """
    Filter tables with no <th> in headers
    :param tableId:
    :return:
    """

    table=readTable(fileName)
    matrix=table.htmlMatrix
    if matrix!=None:
            colHeaders=matrix[table.startRows-1]
            colH=0
            for cell in colHeaders:
                if "<th>" in cell or "<th " in cell:
                    colH+=1
            if colH==0:
                return table.tableId+"\n"

def readDocuments(input=0):
    # for each document that we want to process,
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    for file in files:
        yield file

    yield Pipey.STOP


def processDocuments(fileName):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", fileName)
    result = filterTables(fileName)
    yield result


if __name__ == '__main__':

    args = sys.argv[1:]
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJson"#params.get("json_files")

    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)

    pipeline.add(ResultCombiner('filterTables.out.gz'))
    # One process combines the results into a file.
    pipeline.run(100)




