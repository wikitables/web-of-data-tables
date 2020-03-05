import bz2
import os
import traceback
import sys

import wtables.preprocessing.TableValidator as tableValidator
from wtables.schema.Article import *
from wtables.preprocessing import ReadHTML as readHTML
from wtables.schema.TableType import *
from multiprocessing import Manager
from joblib import Parallel, delayed
import logging

def extractTables(filename, folderOut, cont, dictCount):
    """Extract tables from html.bz, and generate a new file with only tables.
        :param filename: filename bz file.
        :param folderOut: folder where generated files will be saved.
        :param cont: number of file, it will be article ID.
        :param dictCount: save stats of table types.
    """
    fileNameSplit = filename.split("/")
    try:
        file, file_extension = os.path.splitext(fileNameSplit[len(fileNameSplit) - 1])
        if "bz2" not in file_extension:
            return
        print("[Worker %d] File numer %d" % (os.getpid(), cont))
        bzFile = bz2.BZ2File(filename, "rb")
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        title = readHTML.readTitle(soup)
        tables = readHTML.readTables(soup)

        html="<html><head></head><body><h1 class='firstHeading'>{}</h1>".format(title)
        for t in tables:
            tableType=tableValidator.validateHTMLTable(t)
            dictCount[tableType.value]+=1
            logging.debug('dictCount: ' + str(dictCount))
            if (tableType.value==TableType.WIKITABLE.value or
                        tableType.value == TableType.NO_CSS_CLASS.value or
                        tableType.value == TableType.WITH_INNER_TABLE.value):
                html+=str(t)+"<br/>"
                dictCount[TableType.USEFULL_TABLE.value] +=1

        if "</table>" in html or "</TABLE>" in html:
            if folderOut.endswith("/"):
                newFile = bz2.open(folderOut + file + ".bz2", "wt")
            else:
                newFile = bz2.open(folderOut + "/" + file + ".bz2", "wt")
            html+="</body></html>"
            newFile.write(html)
            newFile.close()
    except:
        try:
            logging.debug('Error: '+filename)
        except:
            print("Error name file: ", cont)
        traceback.print_exc()


if __name__ == '__main__':

    args = sys.argv[1:]
    if (len(args) != 2):
        print("Use params <path> folder html files and <pathOutputFolder>")
    else:
        manager = Manager()
        d = manager.dict()
        d[tableValidator.TableType.INFOBOX.value] = 0
        d[tableValidator.TableType.INNER_TABLE.value] = 0
        d[tableValidator.TableType.NO_CSS_CLASS.value] = 0
        d[tableValidator.TableType.WIKITABLE.value] = 0
        d[tableValidator.TableType.FORMAT_BOX.value] = 0
        d[tableValidator.TableType.SMALLTABLE.value] = 0
        d[tableValidator.TableType.TOC.value] = 0
        d[tableValidator.TableType.WITH_INNER_TABLE.value] = 0
        d[tableValidator.TableType.USEFULL_TABLE.value] = 0
        pathZip = args[0]
        pathOutput = args[1]
        logging.basicConfig(filename="./debug.log", level=logging.DEBUG)
        cont = 0
        try:
            files = os.listdir(pathZip)
            Parallel(n_jobs=8, backend="multiprocessing")(
                delayed(extractTables)(pathZip+"/"+fileName, pathOutput, cont, d) for cont, fileName in
                enumerate(files))
            print(d)
        except:
            traceback.print_exc()
