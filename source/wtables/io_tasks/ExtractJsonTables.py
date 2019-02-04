import bz2
import os
import traceback
import sys
import time
from math import ceil
from wtables.schema.TableType import TableType
from wtables.schema.Article import *
from wtables.preprocessing import ReadHTML as readHTML
from wtables.utils import Pipey
from wtables.utils.ResultCombiner import *
from wtables.preprocessing.TextProcessing import TextProcessing

#from multiprocessing import Manager
from joblib import Parallel, delayed
import logging
from wtables.wikidata_db.ConfigProperties import ConfigProperties

def createTables(filename):
    file=filename.split("##$##")[0]
    fileNamebz=file.split("/")[len(file.split("/"))-1]
    cont=int(filename.split("##$##")[1])
    print("cont: ",cont)
    try:
        bzFile = bz2.BZ2File(file, "rb")
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        bzFile.close()
    except:
        print("Error reading file: ", filename)
        return str(cont)+"0\t0\t0\t0\t0\t0\n"
    dictStat={}
    dictStat[TableType.ILL_FORMED.value]=0
    dictStat["NO_PROCESSED"] = 0
    dictStat[TableType.WELL_FORMED.value] = 0
    dictStat[TableType.SMALLTABLE.value] = 0
    dictStat[TableType.WITH_INNER_TABLE.value] = 0
    dictStat[TableType.FORMAT_BOX.value] = 0
    try:
        title = readHTML.readTitle(soup)
        tables = readHTML.readTables(soup)
        tables2d = []
        contTables = 1
        formatTables = 0
        for it, t in enumerate(tables):
            try:
                parents = [p.name for p in t.findParents()]
                if t.parent != None and ("th" in parents or "td" in parents or "tr" in parents):
                    continue
                start=time.time()
                listt2d = readHTML.tableTo2d(t)
                logging.debug("Time reading table: "+ str(time.time()-start))
                validTables = []
                if listt2d == None or len(listt2d) == 0:

                    newTable = readHTML.saveIllTable(t, TableType.ILL_FORMED.value)
                    if newTable != None:
                        validTables.append(newTable)
                        dictStat[TableType.ILL_FORMED.value] += 1
                    else:
                        dictStat["NO_PROCESSED"] += 1

                else:
                    if len(listt2d)>10:
                        validTables.append(newTable)
                        dictStat[TableType.ILL_FORMED.value] += 1
                        continue
                    for t2d in listt2d:
                        if t2d.tableType == TableType.FORMAT_BOX.value:
                            dictStat[TableType.FORMAT_BOX.value] += 1
                            formatTables += 1
                            continue

                        if t2d.tableType == TableType.SMALLTABLE.value:
                            dictStat[TableType.SMALLTABLE.value] += 1
                            continue

                        if t2d.tableType == TableType.ILL_FORMED.value:
                            dictStat[TableType.ILL_FORMED.value] += 1
                            validTables.append(t2d)
                            continue

                        if t2d.tableType == TableType.WITH_INNER_TABLE.value:
                            dictStat[TableType.WITH_INNER_TABLE.value] += 1
                            validTables.append(t2d)
                            continue
                        #print(t2d.toHTML())
                        startRows, colHeaders=readHTML.getMainColHeaders(t2d.htmlMatrix)
                        if startRows>0:
                            startRows,colHeadersType=readHTML.getColHeaderAllLevels(t2d.htmlMatrix, startRows, textProcessing)
                            t2d.setStartRows(startRows)
                            t2d.setColHeaders(colHeadersType)
                            t2d.ncols=len(colHeadersType)
                        validTables.append(t2d)
                        dictStat[TableType.WELL_FORMED.value] += 1

                for t2d in validTables:
                    tableId = str(cont) + "." + str(contTables)
                    t2d.setTableId(tableId)
                    t2d.setArticleId(str(cont))
                    t2d.setArticleTitle(title)
                    t2d.setArticlePath(fileNamebz)

                    tables2d.append(t2d)
                    ft = open(os.path.join(FOLDER_TABLES_OUT,str(tableId.replace(".","_")) + ".json"), "w")
                    ft.write(json.dumps(t2d.reprJSON(), cls=ComplexEncoder, skipkeys=True))
                    ft.close()

                    contTables += 1
            except:
                traceback.print_exc()
                print("Error: ", filename, it)
                continue
        if len(tables2d) > 0:
            article = Article(articleId=str(cont), title=title, tables=tables2d)
            f = open(FOLDER_OUT + "/" + str(cont) + ".json", "w")
            f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
            f.close()
        else:
            if len(tables) == formatTables:
                logging.debug("Format table: " + filename)
            else:
                logging.debug("Error none useful table: " + filename)
        logging.debug(dictStat)
    except:
        traceback.print_exc()
        logging.debug("Error file ", filename)

    return str(cont)+"\t"+  str(dictStat[TableType.ILL_FORMED.value])+"\t"+ \
        str(dictStat["NO_PROCESSED"])+"\t"+ \
        str(dictStat[TableType.WELL_FORMED.value])+"\t"+ \
        str(dictStat[TableType.SMALLTABLE.value])+"\t" + \
        str(dictStat[TableType.WITH_INNER_TABLE.value])+"\t" + \
        str(dictStat[TableType.FORMAT_BOX.value])+"\n"



def readDocuments(input=0):
    # for each document that we want to process,
    path = FOLDER_HTML_FILES
    files = os.listdir(path)

    for i, file in enumerate(files):
        name, file_extension = os.path.splitext(file)
        if file_extension == ".bz2":
            print("IDFILE:"+"\t"+file+"\t"+str(i))
            yield path + "/" + file+"##$##"+str(i)

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    result = createTables(file)
    yield result


if __name__ == '__main__':
    args = sys.argv[1:]
    params = ConfigProperties().loadProperties()
    FOLDER_HTML_FILES ="/home/jhomara/Desktop" #params.get("html_files")
    textProcessing=TextProcessing()
    #if len(args)<2:
    #    print("Use <folderArticlesOut> <folderTablesOut> <fileStatsOut>")
    #else:
    FOLDER_OUT="/home/jhomara/Desktop/prueba" #args[0]
    FOLDER_TABLES_OUT="/home/jhomara/Desktop/prueba/prueba1"#args[1]
    logging.basicConfig(filename="./debug.log", level=logging.DEBUG)
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner("out.out"))
    pipeline.run(100)
