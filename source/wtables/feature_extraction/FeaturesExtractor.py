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


def isValid(t):
    html, t2d = readHTML.tableTo2d(t, str(0))
    if html == "error":
        return False
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


def extractHeaders(filename, cont, queue):
    print("Procesing: ", cont)
    try:
        file, file_extension = os.path.splitext(filename)
        if "bz2" not in file_extension:
            return
        bzFile = bz2.BZ2File(filename, "rb")
        soup = BeautifulSoup(bzFile.read(), 'html.parser')
        tables = readHTML.readTables(soup)
        for contTables, t in enumerate(tables):
            if len(t.find_parents("table")) == 0:
                if (isValid(t)):
                    headers = readHTML.readHeaders(soup)
                    headers = [re.sub('\W+', " ", h) for h in headers]
                    headers = [h.strip() for h in headers]
                    if len(headers) > 0:
                        queue.put(",".join(headers) + "\n")
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

        headersFile = open(pathOutput + "/headersFile.out", "w", encoding="utf-8")

        queue = manager.Queue()
        logging.basicConfig(filename=pathOutput + "/" + LOG_FILENAME, level=logging.DEBUG)

        try:
            for subdir, dirs, files in os.walk(pathZip):
                Parallel(n_jobs=4, backend="multiprocessing")(
                    delayed(extractHeaders)((pathZip + "/" + fileName), cont, queue) for cont, fileName in
                    enumerate(files))

            queue.put("STOP")
            writer_process = multiprocessing.Process(target=Writer,
                                                     args=(pathOutput + "/headersFile.out", queue, "STOP"))
            writer_process.start()
            writer_process.join()

        except:
            traceback.print_exc()
