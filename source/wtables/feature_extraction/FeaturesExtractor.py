import sys, os
import logging
from joblib import Parallel, delayed
import traceback
from multiprocessing import Manager
import re
from bs4 import BeautifulSoup
import io
import bz2
import wtables.preprocessing.ReadHTML as readHTML
import multiprocessing
from wtables.preprocessing.JsonArticle import *

def isValid(t2d):

    if t2d is None:
        return False

    if (len(t2d.cells) == 0):
        return False

    rows, cols = len(t2d.cells), len(t2d.cells[0])
    if t2d.getAttr("class") is None:
        if (cols < 2 or rows < 2):
            return False
        else:
            return True

    tclass = str(t2d.getAttr("class"))
    tclass = re.sub('\W+', " ", tclass)
    if ((tclass.lower() == "infobox")
        or ("infobox" in tclass.lower())):
        return False
    if (tclass == "box"
        or "box" in tclass.lower()
        or "metadata" in tclass.lower()
        or "maptable" in tclass.lower()
        or "vcard" in tclass.lower()):
        return False

    if (cols < 2 or rows < 2):
        return False
    else:
        splitClass = tclass.split()
        for cl in splitClass:
            if cl.strip().startswith("toc"):
                return False
        else:
            return True

def cleanHeader(text):
    text = re.sub("[\(\[].*?[\)\]]", "", text)
    text = text.replace("\t", " ")
    text = text.replace("\n", " ")
    text = re.sub("\W(?:['])?\W", " ", text)
    text = text.replace(","," ")
    text = text.replace(".", " ")
    return text

def extractHeaders(pathZip,filename, cont, queue):
    print("[Worker %d] File numer %d" % (os.getpid(), cont))
    filenamecomplete=pathZip+"/"+filename
    try:
        file, file_extension = os.path.splitext(filename)
        if "bz2" not in file_extension:
            return
        bzFile = bz2.BZ2File(filenamecomplete, "rb")
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        title= readHTML.readTitle(soup)
        tables = readHTML.readTables(soup)
        tables2d=[]
        contTables=1
        for t in tables:
            html, t2d = readHTML.tableTo2d(t, str(cont)+"."+str(contTables))
            if html == "error":
                continue
            if (isValid(t2d)):
                headers = t2d.getHeadersText()
                headers = [cleanHeader(h) for h in headers]
                headers = [h.strip().lower() for h in headers if h!=""]
                headers = set(headers)
                headers=list(headers)
                headers.sort()
                if len(headers) > 0:
                    queue.put(str(cont)+"\t"+filename+"\t"+title+"\t"+str(cont)+"."+str(contTables)+"\t"+",".join(headers) + "\n")
                tables2d.append(t2d)
                contTables+=1
        article = Article(articleId=str(cont), title=title, tables=tables2d)
        f = open(pathOutput + "/" + str(cont) + ".json", "w")
        f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
        f.close()
    except Exception as ex:
        traceback.print_exc()


def Writer(dest_filename, queue, stop_token):
    with io.open(dest_filename, 'w', encoding="utf-8") as dest_file:
        while True:
            line = queue.get()
            if line == stop_token:
                return
            try:
                dest_file.write(line)
            except:
                traceback.print_exc()
                continue


if __name__ == '__main__':
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) != 2):
        print("Use params <path> folder html files and <pathOutputFolder>")
    else:
        manager = Manager()
        pathZip = args[0]
        pathOutput = args[1]

        queue = manager.Queue()
        logging.basicConfig(filename=pathOutput + "/" + LOG_FILENAME, level=logging.DEBUG)

        try:
            #for subdir, dirs, files in os.walk(pathZip):
            files=os.listdir(pathZip)
            Parallel(n_jobs=8, backend="multiprocessing")(
                    delayed(extractHeaders)(pathZip,fileName, cont, queue) for cont, fileName in
                    enumerate(files))

            queue.put("STOP")
            writer_process = multiprocessing.Process(target=Writer,
                                                     args=(pathOutput + "/headersFile.out", queue, "STOP"))
            writer_process.start()
            writer_process.join()

        except:
            traceback.print_exc()
