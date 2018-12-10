import bz2
import os
import traceback
import sys
import numpy as np

from wtables.schema.Article import *
from wtables.utils import Pipey
from wtables.utils.ParseLink import *
from wtables.utils.ResultCombiner import *
from wtables.preprocessing import ReadHTML as readHTML
import wtables.feature_extraction.ProtagonistFeatures as protagonistFeatures
from pathlib import Path


def extractTableInformation(fileName):
    fileNameSplit = fileName.split("/")
    file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
    if "json" not in file_extension:
        return
    file = open(fileName, "r")
    obj = file.read()
    print("Reading file: ", fileName)
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        lineTables=""
        for table in article.tables:
            lineTables+=table.tableId + "\t" + str(table.colHeaders) + "\t" + str(table.rowHeaders) + "\t" + \
                   str(table.ncols)+ "\t" +  str(table.nrows) + "\n"
        return lineTables
    except Exception as ex:
        traceback.print_exc()

def getTablesProtagonist(fileName):
    fileNameSplit = fileName.split("/")
    file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
    if "json" not in file_extension:
        return
    file = open(fileName, "r")
    obj = file.read()
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        lineTables = ""
        for table in article.tables:
            features=protagonistFeatures.extractProtagonistFeatures(table)
            for k, v in features.items():
                lineTables += table.tableId + "\t" + k+"\t"+str(v.get('FRACT_CELL_UNIQ_CONTENT'))+"\t"\
                              +str(v.get('AVG_CELL_WORDS'))+"\t"+str(v.get('DATA_TYPE'))+"\t"+str(v.get('INDEX_POS'))+"\n"
        return lineTables
    except Exception as ex:
        traceback.print_exc()

def getColumnHeaders(tableMatrix):
    startRows, colHeaders = readHTML.getMainColHeaders(tableMatrix)
    startRows += 1

    if len(set(colHeaders)) == 1 and colHeaders[0] == "":
        colHeaders = []
    else:
        colHeaders = [h.lower().strip().replace(" ", "_") + "@" + str(
            readHTML.getColumnType(i, startRows, tableMatrix)) if h != "" else "spancol@" + str(
            readHTML.getColumnType(i, startRows, tableMatrix)) for i, h in enumerate(colHeaders)]
    return colHeaders

def extractLinksFromColumns(fileName):
    filenamesplit = fileName.split("/")
    file, file_extension = os.path.splitext(filenamesplit[len(filenamesplit) - 1])

    if file_extension != ".json":
        return

    file = open(fileName, "r")
    obj = file.read()
    out = ""
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        for table in article.tables:
            tarray = np.array(table.htmlMatrix)
            colHeaders = getColumnHeaders(table.htmlMatrix)#table.colHeaders
            table.colHeaders=colHeaders
            rowHeaders = table.rowHeaders
            setrH = set(rowHeaders)
            if len(setrH) == 1 and "spancol" in rowHeaders[0]:
                rowHeaders = []

            line = table.tableId + "\t" + str(colHeaders) + "\t" + str(
                rowHeaders) + "\t" +  str(len(table.htmlMatrix[0])) + \
                   "\t" + str(len(table.htmlMatrix)-table.startRows) + "\t"
            if len(colHeaders) > 0:
                setcH = set(colHeaders)
                if len(setcH) == 1 and "spancol" in colHeaders[0]:
                    continue
                pairLink = {}
                start = 1  # dictTableInf["nRows"] - dictTableInf["nRowHeaders"]
                for row in range(start, tarray.shape[0]):
                    for colA in range(tarray.shape[1]):
                        contentA = tarray[row][colA]
                        bscell=BeautifulSoup(contentA, "html.parser")
                        tds = bscell.find_all(['td','th'])
                        colspan=tds[0].get("colspan")
                        if colspan!=None and float(colspan)>=100:
                            continue

                        linksA = readHTML.readTableCellLinks(bscell)
                        if len(linksA) == 0:
                            continue
                        else:
                            for colB in range(colA + 1, tarray.shape[1]):
                                if colA == colB or (
                                            tarray[row][colA] == tarray[row][colB]) or (colHeaders[colA]==colHeaders[colB]):
                                    continue
                                contentB = tarray[row][colB]
                                bscell = BeautifulSoup(contentB, "html.parser")
                                tds = bscell.find_all(['td', 'th'])
                                colspan = tds[0].get("colspan")
                                if colspan!=None and float(colspan) >= 100:
                                    continue
                                linksB = readHTML.readTableCellLinks(bscell)
                                key = str(row) + ":" + str(colA) + ":" + str(colB)
                                if len(linksB) == 0:
                                    pairLink[key] = (colHeaders[colA], colHeaders[colB], linksA, [])
                                else:
                                    pairLink[key] = (colHeaders[colA], colHeaders[colB], linksA, linksB)
                write = False
                if len(pairLink) > 0:
                    for k, v in pairLink.items():
                        for la in v[2]:
                            for lb in v[3]:
                                lla = wikiLink(la)
                                llb = wikiLink(lb)
                                if lla != "" and llb != "":
                                    out += line + str(k) + "\t" + v[0] + "\t" + v[1] + "\t" + lla + "\t" + llb + "\n"
                                    write = True
                if write == False:
                    out += line + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\n"
            else:
                if len(table.rowHeaders) > 0:
                    setcH = set(table.rowHeaders)
                    if len(setcH) == 1 and "spancol" in table.rowHeaders[0]:
                        continue
                    out += line + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\n"
        f = open("/home/jluzuria/json_files_fixed/" + filenamesplit[len(filenamesplit) - 1], "w")
        f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
        f.close()
    except:
        print("Error file: ", fileName)
        traceback.print_exc()
    return out


def getColumnWikidataRelations(fileEntities, fileRelations, fileLinks, fileResults):
    # fileResults = open(fileResults, "w")
    fileTableRelations = open("fileTableRelations.out", "w")
    dictEntities = {}
    with open(fileEntities, "r") as fileE:
        for line in fileE:
            splitLine = line.replace("\n", "").split("\t")
            entityLink = splitLine[1].split("/")
            entity = entityLink[len(entityLink) - 1]
            dictEntities[splitLine[0]] = entity

    dictRelations = {}
    with open(fileRelations, "r") as fileR:
        for line in fileR:
            splitLine = line.replace("\n", "").split("\t")
            try:
                relation = splitLine[0] + "#" + splitLine[2]
                if dictRelations.get(relation) == None:
                    dictRelations[relation] = [splitLine[1]]
                else:
                    dictRelations[relation].extend(splitLine[1])
                    dictRelations[relation] = list(set(dictRelations[relation]))
            except:
                print("Error fileRelations", str(splitLine))
    cont = 0
    with open(fileLinks, "r") as fileL:
        for line in fileL:
            print("Line: ", cont)
            cont += 1
            splitLine = line.replace("\n", "").split("\t")
            if len(splitLine) != 10:
                print("Error fileLinks", str(splitLine))
                continue
            col1 = wikiLink(splitLine[8])
            col2 = wikiLink(splitLine[9])
            relationsFound = False
            rel = []
            if col1 != "" and col2 != "":
                col1 = dictEntities.get(col1)
                col2 = dictEntities.get(col2)
                if col1 != None and col2 != None:
                    # dictTables[idTable][1] += 1
                    rel = dictRelations.get(col1 + "#" + col2)
                    rel2 = dictRelations.get(col2 + "#" + col1)
                    if rel != None and rel2 != None:
                        rel.extend(rel2)
                    if rel == None and rel2 != None:
                        rel = rel2
                    if rel != None and len(rel) > 0:
                        rel = list(set(rel))
                        if len(rel) == 1 and rel[0] != "about":
                            relationsFound = True
                            fileTableRelations.write(line.replace("\n", "") + "\t" + str(rel) + "\n")
            rel.clear()
            if relationsFound == False:
                fileTableRelations.write(line.replace("\n", "") + "\t" + "" + "\n")
    fileTableRelations.close()


def readDocuments(input=0):
    # for each document that we want to process,
    path = FOLDER_JSON_FILES
    #files = os.listdir(path)
    #for file in files:
    fin=open(path, "r")
    for line in fin:
        _line=line.replace("\n","")
        pathf=_line.split("/")
        fjson=pathf[len(pathf)-1]
        pathf[0]=""
        pathf="/".join(pathf)
        file=Path("/home/jluzuria/json_files_fixed/"+fjson)
        if file.exists():
            continue
        #name, file_extension = os.path.splitext(file)
        #if file_extension == ".json":
        yield pathf#path + "/" + file

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", file)
    result = extractLinksFromColumns(file)
    yield result


if __name__ == '__main__':
    # fileName="/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/datos/prueba/files1/109332.json"
    # inf=extractLinksFromColumns(fileName)
    # print(inf)
    args = sys.argv[1:]
    # fileEntities=args[0], fileRelations=args[1], fileLinks=args[2], fileResults=args[3]
    # path="/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/datos/wikidata/"
    FOLDER_JSON_FILES = args[0]
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 4)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner(args[1]))
    pipeline.run(100)

