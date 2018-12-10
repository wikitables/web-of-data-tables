import bz2
import os
import traceback
import sys

import wtables.preprocessing.TableValidator as tableValidator
from wtables.schema.Article import *
from wtables.preprocessing import ReadHTML as readHTML

import io
import multiprocessing
from multiprocessing import Manager
from joblib import Parallel, delayed
import logging


def filterTables(filename, folderOut, cont, queue):
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
        tables2d = []
        contTables = 1
        lineOut = ""
        contInnerT=0
        for it, t in enumerate(tables):
            try:
                listt2d = readHTML.tableTo2d(t)
                if listt2d==None:
                    continue
                for t2d in listt2d:
                    if t2d.innerTable!=None and t2d.innerTable:
                        contInnerT+=1
                    else:
                        tableId = str(cont) + "." + str(contTables)
                        if t2d==None:
                            continue
                        if (tableValidator.validateTable(t2d) and tableValidator.validateInnerNavBox(t2d.htmlMatrix)):
                            t2d.setTableId(tableId)
                            colHeaders=t2d.colHeaders
                            rowHeaders = t2d.rowHeaders
                            lineOut += str(cont) + "\t"+file + "\t" + str(cont) + "." + str(
                                    contTables) + "\t" + str(colHeaders) + "\t"+ str(rowHeaders)+"\t"+str(t2d.ncols)+"\t"+str(t2d.nrows)+"\t"+str(t2d.startRows)+"\n"

                            tables2d.append(t2d)
                            contTables += 1
            except:
                print("Error: ", filename, it)
                continue
        if len(tables2d) > 0:
            article = Article(articleId=str(cont), title=title, tables=tables2d)
            f = open(folderOut + "/" + str(cont) + ".json", "w")
            f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
            f.close()
            queue.put(lineOut)
        else:
            print("Error None tables: ", filename)
    except Exception as ex:
        print("Error: ", filename)
        traceback.print_exc()

def Writer(dest_filename, queue, stop_token):
    cont=0
    with io.open(dest_filename, 'w', encoding="utf-8") as dest_file:
        while True:
            line = queue.get()
            print("queue: ",cont)
            if line == stop_token:
                return
            try:
                dest_file.write(line)
            except:
                traceback.print_exc()
                continue
            cont += 1


if __name__ == '__main__':
    LOG_FILENAME = 'debug.log'

    args = sys.argv[1:]
    # fileEntities=args[0], fileRelations=args[1], fileLinks=args[2], fileResults=args[3]
    # columnWikidataRelations(fileEntities, fileRelations, fileLinks, fileResults)

    if (len(args) != 2):
        print("Use params <path> folder html files and <pathOutputFolder>")
    else:
        manager = Manager()
        queue = manager.Queue()
        # iolock = manager.Lock()
        # pool = manager.Pool(4, initializer=Writer, initargs=(queue, iolock))
        pathZip = args[0]
        pathOutput = args[1]
        logging.basicConfig(filename=pathOutput + "/" + LOG_FILENAME, level=logging.DEBUG)

        try:
            files = os.listdir(pathZip)
            Parallel(n_jobs=8, backend="multiprocessing")(
                delayed(filterTables)(pathZip + "/" + fileName, pathOutput, cont, queue) for cont, fileName in
                enumerate(files))

            queue.put("STOP")
            writer_process = multiprocessing.Process(target=Writer,
                                                     args=("headersFile.out", queue, "STOP"))
            writer_process.start()
            writer_process.join()

        except:
            traceback.print_exc()
