import gzip
import sys
import numpy as np
from wtables.utils import Pipey
from wtables.utils.ResultCombiner import *
from wtables.schema.Article import *
import os
import json

def readFile(fileName):
    tableId = fileName.split("/")[len(fileName.split("/")) - 1].replace(".json", "").replace("_", ".")
    file = open(fileName, "r")
    obj = file.read()
    obj = json.loads(obj)
    # Converting json to Table object
    table = ComplexDecoderTable().default(obj)
    return table


def extractNumberClassByColumn(file):
    table=readFile(file)
    if table is None:
        return
    colClassesDict=table.columnClasses
    minColClass=0
    maxColClass=0
    out=""
    setClasses=[]
    for col, classes in colClassesDict.items():
        classesNames=list(classes.keys())
        setClasses.extend(classesNames)
        maxc=len(set(classesNames))
        if maxc>maxColClass:
            maxColClass=maxc
    setClasses=set(setClasses)
    for classe in setClasses:
        out+=table.tableId+"\t"+classe+"\t"+str(maxColClass)+"\n"
    return out


def readDocuments(input=0):
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    cont=0
    for file in files:
        print('File: ', file, cont)
        name, file_extension = os.path.splitext(file)
        if file_extension == ".json":
            yield path + "/" + file
        cont+=1
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    #print("File: ", file)
    return extractNumberClassByColumn(file)


if __name__ == '__main__':
    args = sys.argv[1:]
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJsonEntityClasses"  # params.get("json_files")

    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner(args[0]))

    pipeline.run(100)


