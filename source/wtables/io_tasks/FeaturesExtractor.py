import bz2
import os
import traceback
import sys
import numpy as np
from wtables.wikidata_db.ConfigProperties import ConfigProperties
from wtables.schema.Article import *
from wtables.utils import Pipey
from wtables.utils.ParseLink import *
from wtables.utils.ResultCombiner import *
from wtables.preprocessing import ReadHTML as readHTML
from wtables.schema.TableCell import TableCell

def extractTableInformation(fileName):
    fileNameSplit = fileName.split("/")
    file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
    if "json" not in file_extension:
        return
    file = open(fileName, "r")
    obj = file.read()
    #print("Reading file: ", fileName)
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        lineTables=""
        for table in article.tables:
            #print(table.toHTML())
            if table.attrs is not None:
                classes=table.attrs.get("class")
            else:
                classes=[]
            lineTables+=table.tableId + "\t" + str(table.colHeaders) + "\t" + str(table.rowHeaders) + "\t" + \
                   str(table.ncols)+ "\t" +  str(table.nrows) + "\t"+str(table.startRows)+"\t"+\
            str(table.tableType.value)+"\t"+str(classes)+"\n"
        return lineTables
    except Exception as ex:
        traceback.print_exc()

def getTableProtagonist(articleName):
    return wikiLink(articleName)

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
    out = ""
    try:
        if file_extension != ".json":
            return

        file = open(fileName, "r")
        obj = file.read()


        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        prot=getTableProtagonist(article.title)
        for table in article.tables:
            tarray = np.array(table.htmlMatrix)
            colHeaders = ["protag_article@3"]
            colHeaders.extend(table.colHeaders)
            rowHeaders = table.rowHeaders
            setrH = set(rowHeaders)

            line = table.tableId + "\t" + str(colHeaders) + "\t" +  str(len(table.htmlMatrix[0])) + \
                   "\t" + str(len(table.htmlMatrix)-table.startRows) + "\t"

            if len(colHeaders) > 1:
                setcH = set(colHeaders)
                if len(setcH) == 1 and "spancol" in colHeaders[0]:
                    continue
                pairLink = {}
                start = table.startRows  # dictTableInf["nRows"] - dictTableInf["nRowHeaders"]
                tlinks=[[[] for x in range(tarray.shape[1])] for y in range(len(tarray)-start)]
                rowLink=0
                for row in range(start, tarray.shape[0]):
                    for col in range(tarray.shape[1]):
                        contentA = tarray[row][col]
                        bscell = BeautifulSoup(contentA, "html.parser")
                        linksCell = readHTML.readTableCellLinks(bscell)
                        tlinks[rowLink][col]=linksCell
                    rowLink+=1
                write = False
                for row in range(len(tlinks)):
                    for i in range(len(tlinks[0])):
                            linksR = tlinks[row][i]
                            pos = str(row) + ":" + str(-1) + ":" + str(i)
                            if len(linksR) == 0:
                                continue
                            else:
                                for link in linksR:
                                    _link= wikiLink(link)
                                    if _link is not None and _link!= "" and _link!=prot:
                                        out += line + pos + "\t" + colHeaders[0] + "\t" +colHeaders[i+1] + "\t" + prot + "\t" + _link + "\n"
                                        write=True
                for row in range(len(tlinks)):
                    for i in range(len(tlinks[0])):
                        for j in range(i+1,len(tlinks[0])):
                            pos = str(row) + ":" + str(i) + ":" + str(j)
                            linksL=tlinks[row][i]
                            linksR=tlinks[row][j]
                            if set(linksL)==set(linksR):
                                continue
                            if len(linksL) == 0 or len(linksR) == 0:
                                continue
                            for ll in linksL:
                                for lr in linksR:
                                    lla = wikiLink(ll)
                                    llb = wikiLink(lr)
                                    if lla != "" and llb != "" and lla!=llb:
                                        out += line + pos + "\t" + colHeaders[i+1] + "\t" + colHeaders[j+1] + "\t" + lla + "\t" + llb + "\n"
                                        write=True

                if not write:
                    out += line + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\n"
            else:
                if len(setrH) > 0:
                    if len(setrH) == 1 and "spancol" in table.rowHeaders[0]:
                        continue
                    out += line + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\t" + "" + "\n"
    except:
        print("Error file: ", fileName)
        traceback.print_exc()
    return out


def readDocuments(input=0):
    # for each document that we want to process,
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    for file in files:
        name, file_extension = os.path.splitext(file)
        if file_extension == ".json":
            yield path + "/" + file

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", file)
    result = extractTableInformation(file)
    yield result


if __name__ == '__main__':

    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/articlesJson"#params.get("json_files")
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 12)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner(args[0]))
    pipeline.run(100)



