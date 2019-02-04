import bz2
import os
import traceback
import sys
import numpy as np

from wtables.schema.Article import *
from wtables.wikidata_db.WikidataDAO import *
from wtables.utils.ResultCombiner import *
from wtables.preprocessing import ReadHTML as readHTML
from wtables.schema.TableCell import TableCell
from wtables.schema.TableType import TableType
from wtables.preprocessing.TextProcessing import TextProcessing
import time
from wtables.utils import Pipey
from wtables.utils.ResultCombiner import *
import logging
from wtables.wikidata_db.ConfigProperties import ConfigProperties
import re
from wtables.utils.ParseLink import *

def updateJsonFile(fileName):
    fileNameSplit = fileName.split("/")
    file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
    if "json" not in file_extension:
        return
    jsonFile = open(fileName, "r")
    obj = jsonFile.read()

    #stemmer = SnowballStemmer("english")
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        lineTables = ""
        tables2D=[]
        out=""
        for table in article.tables:

            if table.tableType==None or table.tableType.value=="":
                table.setColHeaders([])
                table.setStartRows(0)
                tables2D.append(table)
                continue
            if table.tableType.value != TableType.WELL_FORMED.value:
                table.setTableType(table.tableType.value)
                table.setColHeaders([])
                table.setStartRows(0)
                tables2D.append(table)
                continue
            else:
                try:
                    startRow, headers = readHTML.getMainColHeaders(table.htmlMatrix)
                except Exception as ex:
                    table.setTableType(table.tableType.value)
                    table.setColHeaders([])
                    table.setStartRows(0)
                    tables2D.append(table)
                    continue

                if startRow==0:
                    table.setTableType(table.tableType.value)
                    table.setColHeaders([])
                    table.setStartRows(startRow)
                    tables2D.append(table)
                    continue
                table.setStartRows(startRow)


                #startRow = int(table.startRows)
                matrix = np.array(table.htmlMatrix)
                listOfLevelHeaders = []
                for i in range(startRow):
                    listOfLevelHeaders.append(matrix[i])
                headersMatch = []
                for row in listOfLevelHeaders:
                    cleanTagHeaders = []
                    for col in range(len(row)):
                        cell = BeautifulSoup(row[col], "html.parser")
                        cell=readHTML.cleanTableCellTag(cell)
                        text = " ".join([s for s in cell.strings if s.strip('\n ') != ''])
                        text = text.replace("*", "").replace("@","")
                        cleanTagHeaders.append(text)
                        cleanTagHeaders = [textProcessing.cleanCellHeader(h) for h in cleanTagHeaders]
                    headersMatch.append(cleanTagHeaders)
                lastRow = headersMatch[len(headersMatch) - 1]
                headersMatch[len(headersMatch) - 1] = ['spancol' if h == '' else h for h in lastRow]
                newHeader=[]
                for col in range(len(headersMatch[0])):
                    textCol = headersMatch[0][col]
                    for row in range(1, len(headersMatch)):
                        textCol += "**" + headersMatch[row][col]
                    newHeader.append(textCol)
                newHeader=[re.sub('^\\**', '',h) for h in newHeader]
                if startRow>1:
                    newHeader=[h[:-2] if h.endswith("**") else h for h in newHeader]
                newHeader=textProcessing.orderHeaders(newHeader)
                newHeaderType = []
                for i, col in enumerate(newHeader):
                    type = readHTML.getColumnType(i, startRow, table.htmlMatrix)
                    newHeaderType.append(newHeader[i] + "@" + str(type))
                table.setColHeaders(newHeaderType)
                table.ncols=len(newHeaderType)
                table.setTableType(table.tableType.value)
                tables2D.append(table)
                try:
                    out=extractLinks2(article.title,table)
                except Exception as ex1:
                    print("Error links extraction: ", table.tableId)
                    traceback.print_exc()

        article.setTables(tables2D)
        #article = Article(articleId=article., title=title, tables=tables2d)
        f = open(FOLDER_OUT+ "/" + file+ ".json", "w")
        f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
        f.close()


        return out
    except Exception as ex:
        print("Error File: ", file)
        traceback.print_exc()




def extractLinks2(articleTitle, table):
    out=""
    tarray=np.array(table.htmlMatrix)
    start=table.startRows
    colHeaders=table.colHeaders
    #colHeaders = ["protag_article@3"]
    #colHeaders.extend(table.colHeaders)
    line = table.tableId + "\t" + str(colHeaders) + "\t" + str(len(tarray[0])) + \
           "\t" + str(len(tarray) - table.startRows) + "\t"
    prot=wikiLink(articleTitle)
    pwd=wikidataDAO.getWikidataID(prot)
    nrows=len(tarray)-start
    if pwd==None:
        pwd=""
    dictColTables={}
    if len(colHeaders) > 1:
        pairLink = {}
        tlinks = [[[] for x in range(tarray.shape[1])] for y in range(len(tarray) - start)]
        rowLink = 0
        for row in range(start, tarray.shape[0]):
            for col in range(tarray.shape[1]):
                contentA = tarray[row][col]
                bscell = BeautifulSoup(contentA, "html.parser")
                linksCell = readHTML.readTableCellLinks(bscell)
                tlinks[rowLink][col] = linksCell
            rowLink += 1
        write = False
        protagonist="protag_article@3"
        for row in range(len(tlinks)):
            for i in range(len(tlinks[0])):
                colName2=colHeaders[i]
                linksR = tlinks[row][i]
                key=protagonist+"##"+colName2

                pos = str(start)+":"+ str(row+start) + ":" + str(-1) + ":" + str(i)
                if len(linksR) == 0:
                    continue
                else:
                    for link in linksR:
                        _link = wikiLink(link)
                        if _link != None and _link != "" and _link != prot:
                            wd=wikidataDAO.getWikidataID(_link)
                            if wd==None:
                                wd=""
                            props=[]
                            if pwd!="" and wd!="":
                                if dictColTables.get(key) == None:
                                    dictColTables[key] = {}
                                props=wikidataDAO.getRelations(pwd, wd)
                            if len(props)>0:
                                    for p in props:
                                        contp=dictColTables.get(key).get(p)
                                        if contp==None:
                                            dictColTables[key][p]=1/nrows
                                        else:
                                            dictColTables[key][p]+=(1/nrows)
                                        out += line + pos + "\t" + protagonist+ "\t" + colName2 + "\t" + prot + "\t"+ _link + "\t"+\
                                        pwd+"\t"+wd+"\t"+p+"\t"+str(props)+"\n"
                            else:
                                    out += line + pos + "\t" + protagonist+ "\t" + colName2 + "\t" + prot + "\t"+ _link + "\t"+\
                                        pwd+"\t"+wd+"\t"+""+"\n"

        for row in range(len(tlinks)):
            for i in range(len(tlinks[0])):
                for j in range(i + 1, len(tlinks[0])):
                    pos = str(start)+":"+str(row+start) + ":" + str(i) + ":" + str(j)
                    linksL = tlinks[row][i]
                    linksR = tlinks[row][j]
                    colName1=colHeaders[i]
                    colName2=colHeaders[j]
                    key=colName1 + "##" + colName2

                    if set(linksL) == set(linksR):
                        continue
                    if len(linksL) == 0 or len(linksR) == 0:
                        continue
                    for ll in linksL:
                        for lr in linksR:
                            lla = wikiLink(ll)
                            llb = wikiLink(lr)
                            if lla != "" and llb != "" and lla != llb:
                                wd1 = wikidataDAO.getWikidataID(lla)
                                if wd1 == None:
                                    wd1 = ""
                                wd2 = wikidataDAO.getWikidataID(llb)
                                if wd2 == None:
                                    wd2 = ""
                                props=[]
                                if wd1 != "" and wd2 != "":
                                    if dictColTables.get(key)==None:
                                        dictColTables[key] = {}
                                    props = set(wikidataDAO.getRelations(wd1, wd2))
                                if len(props) > 0:
                                    for p in props:
                                        contp = dictColTables.get(key).get(p)
                                        if contp == None:
                                            dictColTables[key][p] = 1/nrows
                                        else:
                                            dictColTables[key][p] += (1/nrows)
                                        out += line + pos + "\t" + colName1 + "\t" + colName2+ \
                                                       "\t" + lla + "\t" + llb + "\t" + wd1 + "\t" + wd2+"\t"+p+"\t"+str(props)+"\n"
                                else:
                                    out += line + pos + "\t" + colName1 + "\t" + colName2 + \
                                            "\t" + lla + "\t" + llb + "\t" + wd1 + "\t" + wd2 + "\t" + "" + "\n"
        if len(dictColTables)!=0:
            return out+"RESULT"+ table.tableId+"\t"+str(dictColTables)+"\n"
        else:
            return out
def extractRelations(fileName):
    fileNameSplit = fileName.split("/")
    file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
    if "json" not in file_extension:
        return
    jsonFile = open(fileName, "r")
    obj = jsonFile.read()

    #stemmer = SnowballStemmer("english")
    try:
        obj = json.loads(obj)
        article = ComplexDecoder().default(obj)
        lineTables = ""
        tables2D=[]
        out=""
        for table in article.tables:
            if table.tableType==None or table.tableType.value=="":
                continue
            if table.tableType.value != TableType.WELL_FORMED.value:
                continue
            else:
                startRow=table.startRows
                headers=table.colHeaders
                if startRow==0:
                    continue
                #startRow = int(table.startRows)
                matrix = np.array(table.htmlMatrix)
                try:
                    outl, dictRel=extractLinks(article.title,table)
                    out+=outl
                except Exception as ex1:
                    print("Error links extraction: ", table.tableId)
                    traceback.print_exc()
        return out
    except Exception as ex:
        print("Error File: ", file)
        traceback.print_exc()


def extractLinks(articleTitle, table):
    out=""
    tarray=np.array(table.htmlMatrix)
    start=table.startRows
    colHeaders=table.colHeaders
    #colHeaders = ["protag_article@3"]
    #colHeaders.extend(table.colHeaders)
    line = table.tableId + "\t" + str(colHeaders) + "\t" + str(len(tarray[0])) + \
           "\t" + str(len(tarray) - table.startRows) + "\t"
    prot=wikiLink(articleTitle)
    pwd=wikidataDAO.getWikidataID(prot)
    if pwd==None:
        pwd=""
    if len(colHeaders) > 1:
        pairLink = {}
        tlinks = [[[] for x in range(tarray.shape[1])] for y in range(len(tarray) - start)]
        rowLink = 0
        for row in range(start, tarray.shape[0]):
            for col in range(tarray.shape[1]):
                contentA = tarray[row][col]
                bscell = BeautifulSoup(contentA, "html.parser")
                linksCell = readHTML.readTableCellLinks(bscell)
                tlinks[rowLink][col] = linksCell
            rowLink += 1
        write = False

        for row in range(len(tlinks)):
            for i in range(len(tlinks[0])):
                nameCol2=colHeaders[i]
                linksR = tlinks[row][i]
                pos = str(start)+":"+ str(row+start) + ":" + str(-1) + ":" + str(i)
                if len(linksR) == 0:
                    continue
                else:
                    for link in linksR:
                        _link = wikiLink(link)
                        if _link != None and _link != "" and _link != prot:
                            wd=wikidataDAO.getWikidataID(_link)
                            if wd==None:
                                wd=""
                            props=[]
                            if pwd!="" and wd!="":
                                props=wikidataDAO.getRelations(pwd, wd)
                            if len(props)>0:
                                    for p in props:
                                        out += line + pos + "\t" + "protag_article@3"+ "\t" + nameCol2 + "\t" + prot + "\t"+ _link + "\t"+\
                                        pwd+"\t"+wd+"\t"+p+"\n"
                            else:
                                    out += line + pos + "\t" + "protag_article@3"+ "\t" + nameCol2 + "\t" + prot + "\t"+ _link + "\t"+\
                                        pwd+"\t"+wd+"\t"+""+"\n"

        for row in range(len(tlinks)):
            for i in range(len(tlinks[0])):
                for j in range(i + 1, len(tlinks[0])):
                    pos = str(start)+":"+str(row+start) + ":" + str(i) + ":" + str(j)
                    linksL = tlinks[row][i]
                    linksR = tlinks[row][j]
                    if set(linksL) == set(linksR):
                        continue
                    if len(linksL) == 0 or len(linksR) == 0:
                        continue
                    for ll in linksL:
                        for lr in linksR:
                            lla = wikiLink(ll)
                            llb = wikiLink(lr)
                            if lla != "" and llb != "" and lla != llb:
                                wd1 = wikidataDAO.getWikidataID(lla)
                                if wd1 == None:
                                    wd1 = ""
                                wd2 = wikidataDAO.getWikidataID(llb)
                                if wd2 == None:
                                    wd2 = ""
                                props=[]
                                if wd1 != "" and wd2 != "":
                                    props = wikidataDAO.getRelations(wd1, wd2)
                                if len(props) > 0:
                                    for p in props:
                                        out += line + pos + "\t" + colHeaders[i] + "\t" + colHeaders[j] + \
                                                       "\t" + lla + "\t" + llb + "\t" + wd1 + "\t" + wd2+"\t"+p+"\n"
                                else:
                                    out += line + pos + "\t" + colHeaders[i] + "\t" + colHeaders[j] + \
                                            "\t" + lla + "\t" + llb + "\t" + wd1 + "\t" + wd2 + "\t" + "" + "\n"
        return out




def extractLinksGenerator(articleTitle, table):
    out=""

    tarray=np.array(table.htmlMatrix)
    start=table.startRows
    colHeaders=table.colHeaders
    #colHeaders = ["protag_article@3"]
    #colHeaders.extend(table.colHeaders)
    line = table.tableId + "\t" + str(colHeaders) + "\t" + str(len(tarray[0])) + \
           "\t" + str(len(tarray) - table.startRows) + "\t"
    prot=wikiLink(articleTitle)
    pwd=wikidataDAO.getWikidataID(prot)
    if pwd==None:
        pwd=""
    if len(colHeaders) > 1:
        pairLink = {}
        tlinks = [[[] for x in range(tarray.shape[1])] for y in range(len(tarray) - start)]
        rowLink = 0
        for row in range(start, tarray.shape[0]):
            for col in range(tarray.shape[1]):
                contentA = tarray[row][col]
                bscell = BeautifulSoup(contentA, "html.parser")
                linksCell = readHTML.readTableCellLinks(bscell)
                tlinks[rowLink][col] = linksCell
            rowLink += 1
        write = False

        dictRelByTable={}
        for i in range(len(tlinks[0])):
            nameCol2 = colHeaders[i]
            dictRelCount = {}
            for row in range(len(tlinks)):
                linksR = tlinks[row][i]
                pos = str(start)+":"+ str(row+start) + ":" + str(-1) + ":" + str(i)
                if len(linksR) == 0:
                    continue
                else:
                    for link in linksR:
                        _link = wikiLink(link)
                        if _link != None and _link != "" and _link != prot:
                            wd=wikidataDAO.getWikidataID(_link)
                            if wd==None:
                                wd=""
                            props=[]
                            if pwd!="" and wd!="":
                                props=wikidataDAO.getRelations(pwd, wd)
                            if len(props)>0:
                                for p in props:
                                    v=dictRelCount.get(p)
                                    if v==None:
                                        dictRelCount[p]=1
                                    else:
                                        dictRelCount[p]+=1
                                yield  {cols:"protag_article@3##"  + nameCol2,
                                        entity1: prot + " :"+pwd,
                                        entity2: _link+ " :"+wd,
                                        relations: props}
            dictRelByTable['protag_article@3##' + nameCol2] = dictRelCount
        for i in range(len(tlinks[0])):
            for j in range(i + 1, len(tlinks[0])):
                nameCol1=colHeaders[i]
                nameCol2 = colHeaders[j]
                dictRelCount = {}
                for row in range(len(tlinks)):
                    pos = str(start)+":"+str(row+start) + ":" + str(i) + ":" + str(j)
                    linksL = tlinks[row][i]
                    linksR = tlinks[row][j]
                    if set(linksL) == set(linksR):
                        continue
                    if len(linksL) == 0 or len(linksR) == 0:
                        continue
                    for ll in linksL:
                        for lr in linksR:
                            lla = wikiLink(ll)
                            llb = wikiLink(lr)
                            if lla != "" and llb != "" and lla != llb:
                                wd1 = wikidataDAO.getWikidataID(lla)
                                if wd1 == None:
                                    wd1 = ""
                                wd2 = wikidataDAO.getWikidataID(llb)
                                if wd2 == None:
                                    wd2 = ""
                                props=[]
                                if wd1 != "" and wd2 != "":
                                    props = wikidataDAO.getRelations(wd1, wd2)
                                if len(props) > 0:
                                    for p in props:
                                        v = dictRelCount.get(p)
                                        if v == None:
                                            dictRelCount[p] = 1
                                        else:
                                            dictRelCount[p] += 1
                                    yield {cols: "protag_article@3##" + nameCol2,
                                           entity1: lla + " :" + wd1,
                                           entity2: llb+ " :" + wd2,
                                           relations: props}
                dictRelByTable[nameCol1+'##' + nameCol2] = dictRelCount
        return out, dictRelByTable


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
    result = updateJsonFile(file)
    if result!=None and result!="":
        yield result


if __name__ == '__main__':
    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/jsonArticles"#params.get("json_files")
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillData()
    textProcessing=TextProcessing()

    #FILE_INPUT=args[0]
    FOLDER_OUT=args[0]
    
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 12)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner(args[1], args[2]))
    pipeline.run(100)

