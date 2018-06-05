# http://emunoz.org/publications/files/LD4IE2013.pdf
# Classification Tables: https://github.com/emir-munoz/wikitables

from bs4 import BeautifulSoup
import sys, os
import wtables.preprocessing.ReadHTML as readHTML
import bz2
import logging
import traceback
from wtables.preprocessing.JsonArticle import *
from joblib import Parallel, delayed
import multiprocessing
import io

def processFile(pathZip, pathOutput, filename, cont, queue, queueError):
    try:
        logging.debug('Open File: ' + str(cont) + "> " + filename)
        file, file_extension = os.path.splitext(filename)
        if "bz2" not in file_extension:
            return
        #filename=filename.decode('utf8')
        bzFile = bz2.BZ2File(pathZip + "/" + filename, "rb")

        usefulTables = 0
        wellFormedTables = 0
        notWellFormedTables = 0
        tocTables = 0
        infoboxTables = 0
        smallDimTables = 0
        otherTables = 0
        wikiTables = 0
        tracklist=0
        totalTables=0
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        title = readHTML.readTitle(soup)
        tables = readHTML.readTables(soup)
        tables2d = []
        otherClases={}
        for contTables,t in enumerate(tables):
            html, t2d = readHTML.tableTo2d(t, str(cont) + "." + str(contTables))
            if html=="error":
                continue
            else:
                totalTables+=1

            if t2d is None:
                notWellFormedTables += 1
                continue
            else:
                wellFormedTables += 1

            if len(t2d.cells) == 0:
                otherTables += 1
                continue

            if t2d.getAttr("class") is None:
                rows, cols = len(t2d.cells), len(t2d.cells[0])
                if (cols < 2 or rows < 2):
                    smallDimTables += 1
                    continue

                if (cols >= 2 and rows >= 2):
                    usefulTables += 1
                    tables2d.append(t2d)
                    continue

                otherClases+=1
                continue

            tclass = str(t2d.getAttr("class"))
            if ((tclass.lower() == "toc")
                or ("toc" in tclass.lower())):
                tocTables += 1
                continue
            if ((tclass.lower() == "infobox")
                or ("infobox" in tclass.lower())):
                infoboxTables += 1
                continue
            if (tclass == "box"
                or "box" in tclass.lower()
                or "metadata" in tclass.lower()
                or "maptable" in tclass.lower()
                or "vcard" in tclass.lower()):
                otherTables += 1
                continue

            rows, cols = len(t2d.cells), len(t2d.cells[0])
            if (cols < 2 or rows < 2):
                smallDimTables += 1
                continue

            if (cols >= 2 and rows >= 2):
                usefulTables += 1
                if (tclass == "wikitable"
                    or "wikitable" in tclass):
                    wikiTables += 1
                else:
                    if (tclass == "tracklist"
                        or "tracklist" in tclass):
                        tracklist += 1
                    else:
                        if otherClases.get(tclass) is None:
                            otherClases[tclass] = 1
                        else:
                            val = otherClases.get(tclass)
                            val = int(val) + 1
                            otherClases[tclass] = val
                tables2d.append(t2d)
                continue
            otherTables += 1

        queue.put(str(cont) + "\t" + filename +  "\t" + str(totalTables) + "\t" + str(
                wellFormedTables) + "\t"
                               + str(notWellFormedTables) + "\t" + str(tocTables) + "\t" + str(
                infoboxTables) + "\t" + str(
                smallDimTables) + "\t"
                               + str(otherTables) + "\t" + str(usefulTables) + "\t" + str(wikiTables)+"\t" + str(tracklist)+"\t"
                  +str(otherClases)+"\n")

        if len(tables2d) > 0:
            article = Article(articleId=str(cont), title=title, tables=tables2d)
            f = open(pathOutput + "/" + str(cont) + ".json", "wb")
            f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
            f.close()


    except Exception as ex:
        queueError.put(filename+"\n")
        return

def Writer(dest_filename, queue, some_stop_token):
    with io.open(dest_filename, 'wb') as dest_file:
        while True:
            line = queue.get()
            if line == some_stop_token:
                return
            dest_file.write(line.encode("utf-8"))

def WriterErrorFile(dest_filename, queue, some_stop_token):
    with io.open(dest_filename, 'wb') as dest_file:
        while True:
            line = queue.get()
            if line == some_stop_token:
                return
            dest_file.write(line.encode("utf-8"))

if __name__ == '__main__':
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) !=2):
        print("Use params <path> folder Zip files and <pathOutputFolder>")
    else:
        pathZip = args[0]
        pathOutput = args[1];
        logging.basicConfig(filename=pathOutput + "/" + LOG_FILENAME, level=logging.DEBUG)

        m = multiprocessing.Manager()
        queue = m.Queue()
        queueError = m.Queue()
        queue.put("num" + "\t" +
                         "filename" + "\t" + "totalTables" + "\t" + "wellFormedTables" + "\t"
                         + "notWellFormedTables" + "\t" + "tocTables" + "\t" + "infoboxTables" + "\t"
                         + "smallDimTables" + "\t"
                         + "otherTables" + "\t" + "usefulTables" + "\t" + "wikiTables" + "\t" + "tracklistTables" +"\t" + "otherUsefulTables"+"\n")
        try:
            for subdir, dirs, files in os.walk(pathZip):
                Parallel(n_jobs=4, backend="multiprocessing")(delayed(processFile)(pathZip, pathOutput, fileName, cont, queue, queueError) for cont,fileName in enumerate(files))

            queue.put("STOP")
            queueError.put("STOP")
            writer_process = multiprocessing.Process(target=Writer, args=(pathOutput+"/classFile.csv", queue, "STOP"))
            writer_process.start()
            writer_process.join()

            writer_error_process = multiprocessing.Process(target=WriterErrorFile,
                                                     args=(pathOutput + "/filesError.out", queueError, "STOP"))
            writer_error_process.start()
            writer_error_process.join()
        except:
            traceback.print_exc()
