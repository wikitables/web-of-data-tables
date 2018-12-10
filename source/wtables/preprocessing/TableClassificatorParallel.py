# http://emunoz.org/publications/files/LD4IE2013.pdf
# Classification Tables: https://github.com/emir-munoz/wikitables

import sys, os
import wtables.preprocessing.ReadHTML as readHTML
import bz2
import logging
import traceback
from wtables.schema.Article import *
from joblib import Parallel, delayed
import multiprocessing
import io
import shutil
import re

def processFile(pathZip, pathOutput, filename, cont, queue, queueError):
    try:
        logging.debug('Open File: ' + str(cont) + "> " + filename)
        file, file_extension = os.path.splitext(filename)
        if "bz2" not in file_extension:
            return
        bzFile = bz2.BZ2File(pathZip + "/" + filename, "rb")
        usefulTables = 0
        wellFormedTables = 0
        notWellFormedTables = 0
        tocTables = 0
        infoboxTables = 0
        smallDimTables = 0
        otherTables = 0
        wikiTables = 0
        tracklist = 0
        totalTables = 0
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        #title = readHTML.readTitle(soup)
        tables = readHTML.readTables(soup)
        tables2d = []
        withInnerTables=0
        tocClases={}
        otherClasses={}
        otherUsefulClases = {}
        for contTables, t in enumerate(tables):
            html, t2d = readHTML.tableTo2d(t, str(cont) + "." + str(contTables))
            if html == "error":
                continue
            else:
                totalTables += 1

            if t2d is None:
                notWellFormedTables += 1
                continue
            else:
                wellFormedTables += 1

            if len(t2d.cells) == 0:
                otherTables += 1
                continue

            rows, cols = len(t2d.cells), len(t2d.cells[0])
            if t2d.getAttr("class") is None:
                if (cols < 2 or rows < 2):
                    smallDimTables += 1
                    continue
                else:
                    usefulTables += 1
                    tables2d.append(t2d)
                    continue

            tclass = str(t2d.getAttr("class"))
            tclass =re.sub('\W+', " ", tclass)
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
                if otherClasses.get(tclass) is None:
                    otherClasses[tclass] = 1
                else:
                    val = otherClasses.get(tclass)
                    val = int(val) + 1
                    otherClasses[tclass] = val
                continue

            rows, cols = len(t2d.cells), len(t2d.cells[0])
            if (cols < 2 or rows < 2):
                smallDimTables += 1
                continue

            if (cols >= 2 and rows >= 2):
                if (tclass == "wikitable"
                    or "wikitable" in tclass.lower()):
                    usefulTables += 1
                    wikiTables += 1
                else:
                    if (tclass == "tracklist"
                        or "tracklist" in tclass.lower()):
                        tracklist += 1
                        usefulTables+=1
                    else:
                        splitClass = tclass.split()
                        toc=False
                        for cl in splitClass:
                            if cl.strip().startswith("toc"):
                                toc=True
                                tocTables += 1
                                if tocClases.get(tclass) is None:
                                    tocClases[tclass] = 1
                                else:
                                    val = tocClases.get(tclass)
                                    val = int(val) + 1
                                    tocClases[tclass] = val
                                break
                        if toc==True:
                            continue

                        usefulTables+=1
                        if otherUsefulClases.get(tclass) is None:
                            otherUsefulClases[tclass] = 1
                        else:
                            val = otherUsefulClases.get(tclass)
                            val = int(val) + 1
                            otherUsefulClases[tclass] = val

                if len(t.findChildren(['table'])) >= 1:
                    withInnerTables += 1
                tables2d.append(t2d)
                continue
            otherTables += 1

        queue.put(str(cont) + "\t" + filename + "\t" + str(totalTables) + "\t" + str(
            wellFormedTables) + "\t"
                  + str(notWellFormedTables) + "\t" + str(tocTables) + "\t"+ str(tocClases) +"\t"+ str(
            infoboxTables) + "\t" + str(
            smallDimTables) + "\t"
                  + str(otherTables) + "\t" + str(otherClasses) +"\t"+ str(usefulTables) + "\t"+ str(withInnerTables)+"\t" + str(wikiTables) + "\t" + str(tracklist) + "\t"
                  + str(otherUsefulClases) + "\n")

        if len(tables2d) > 0:
            shutil.copy2(pathZip + "/" + filename, pathOutput)
            #article = Article(articleId=str(cont), title=title, tables=tables2d)
            #f = open(pathOutput + "/" + str(cont) + ".json", "w")
            #f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
            #f.close()


    except Exception as ex:
        queueError.put(filename + "\n")
        traceback.print_exc()
        return


def Writer(dest_filename, queue, some_stop_token):
    with io.open(dest_filename, 'w', encoding="utf-8") as dest_file:
        while True:
            line = queue.get()
            if line == some_stop_token:
                return
            try:
                dest_file.write(line)
            except:
                traceback.print_exc()
                continue


def WriterErrorFile(dest_filename, queue, some_stop_token):
    with io.open(dest_filename, 'w', encoding="utf-8") as dest_file:
        while True:
            line = queue.get()
            if line == some_stop_token:
                return
            try:
                dest_file.write(line)
            except:
                continue


if __name__ == '__main__':
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) != 2):
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
                  + "notWellFormedTables" + "\t" + "tocTables" + "\t" +"tocClassTables" + "\t"+ "infoboxTables" + "\t"
                  + "smallDimTables" + "\t"
                  + "otherTables" + "\t" + "otherClassTables" + "\t"+ "usefulTables" + "\t"+ "withInnerTables"+"\t" + "wikiTables" + "\t" + "tracklistTables" + "\t" + "otherUsefulTables" + "\n")
        try:
            for subdir, dirs, files in os.walk(pathZip):
                Parallel(n_jobs=4, backend="multiprocessing")(
                    delayed(processFile)(pathZip, pathOutput, fileName, cont, queue, queueError) for cont, fileName in
                    enumerate(files))

            queue.put("STOP")
            queueError.put("STOP")
            writer_process = multiprocessing.Process(target=Writer, args=(pathOutput + "/classFile.csv", queue, "STOP"))
            writer_process.start()
            writer_process.join()

            writer_error_process = multiprocessing.Process(target=WriterErrorFile,
                                                           args=(pathOutput + "/filesError.out", queueError, "STOP"))
            writer_error_process.start()
            writer_error_process.join()
        except:
            traceback.print_exc()
