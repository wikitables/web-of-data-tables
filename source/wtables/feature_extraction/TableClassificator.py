# http://emunoz.org/publications/files/LD4IE2013.pdf
# Classification Tables: https://github.com/emir-munoz/wikitables

from bs4 import BeautifulSoup
import sys,os
# sys.path.insert(0, '/home/jluzuria/web-of-data-tables')
import wtables.preprocessing.ReadHTML as readHTML
import bz2
import logging
import traceback
import re
from wtables.preprocessing.JsonArticle import *

class TableClassificator(object):

    def classify(self):

        LOG_FILENAME = 'debug.log'
        args = sys.argv[1:]

        if (len(args) < 2):
            print("Use params <path> folder Zip files and <pathOutputFolder>")
            return
        pathZip = args[0]
        pathOutput = args[1];
        logging.basicConfig(filename=pathOutput+"/"+LOG_FILENAME, level=logging.DEBUG)
        classFile = open(pathOutput + "/classFile.out", "wb")
        headersFile = open(pathOutput + "/headersFile.out", "wb")
        filesNoProcessed = open(pathOutput + "/filesNoProcessed.out", "w")

        headersFile.write(("num"+"\t"+"filename" + "\t" +"title" + "\t" + "headers" + "\n").encode("utf8"))
        classFile.write(("num"+"\t"+
            "filename" + "\t"+"title" + "\t" + "totalTables" + "\t" + "wellFormedTables" + "\t"
             + "notWellFormedTables" + "\t" + "tocTables" + "\t" + "infoboxTables" + "\t"
                +"smallDimTables" + "\t"
             + "otherTables" + "\t" + "usefulTables" + "\t" + "wikiTables" + "\n").encode("utf8"))
        cont=1
        otherClases={}
        try:
            for subdir, dirs, files in os.walk(pathZip):
                for filename in files:
                    try:
                        logging.debug('Open File: ' + str(cont) + "> " + filename)
                        file, file_extension = os.path.splitext(filename)
                        if "bz2" not in file_extension:
                            continue
                        bzFile = bz2.BZ2File(pathZip + "/" + filename, "r")
                        usefulTables = 0
                        wellFormedTables = 0
                        notWellFormedTables = 0
                        tocTables = 0
                        infoboxTables = 0
                        smallDimTables = 0
                        otherTables = 0
                        wikiTables = 0

                        soup = BeautifulSoup(bzFile.read(), 'html.parser')
                        title = readHTML.readTitle(soup)
                        tables = readHTML.readTables(soup)
                        tables2d=[]
                        contTables=1
                        for t in tables:
                            html, t2d = readHTML.tableTo2d(t,str(cont)+"."+str(contTables))
                            if t2d is None:
                                notWellFormedTables += 1
                                continue
                            else:
                                wellFormedTables += 1
                            if t2d.getAttr("class") is None:
                                otherTables += 1
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
                            if (tclass == "navbox"
                                or "navbox" in tclass.lower()
                                or "metadata" in tclass.lower()
                                or "maptable" in tclass.lower()
                                or "vcard" in tclass.lower()
                                or "mbox" in tclass.lower()):
                                otherTables += 1
                                continue

                            if len(t2d.cells) == 0:
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
                                    if otherClases.get(tclass) is None:
                                        otherClases[tclass]=1
                                    else:
                                        val=otherClases.get(tclass)
                                        val=int(val)+1
                                        otherClases[tclass]=val
                                stringHeaders = ""
                                headers = readHTML.readHeaders(t)
                                for h in headers:
                                    h = h.replace('\t', "")
                                    h = re.sub('\W+', ' ', h)
                                    if h != "" and h != " ":
                                        stringHeaders += h.strip() + ","
                                if stringHeaders != "":
                                    headersFile.write((str(
                                        cont) + "\t" + filename + "\t" + title + "\t" + stringHeaders + "\n").encode(
                                        "utf8"))
                                tables2d.append(t2d)
                                continue
                            otherTables += 1
                            contTables+=1

                        classFile.write((str(cont)+"\t"+filename + "\t"+title + "\t" + str(len(tables)) + "\t" + str(wellFormedTables) + "\t"
                                 + str(notWellFormedTables) + "\t" + str(tocTables) + "\t" + str(infoboxTables) + "\t" + str(
                                    smallDimTables) + "\t"
                                 + str(otherTables) + "\t" + str(usefulTables) + "\t" + str(wikiTables) + "\n").encode("utf8"))
                        if len(tables2d)>0:
                            article = Article(articleId=str(cont), title=title, tables=tables2d)
                            f = open(pathOutput + "/" + str(cont) + ".json", "w")
                            f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
                            f.close()
                        cont+=1


                    except Exception as ex:
                        print(filename)
                        logging.debug(ex)
                        try:
                            filesNoProcessed.write(filename+"\n")
                        except:
                            logging.debug("File Name can't write:"+str(cont))
                            continue

        except:
            traceback.print_exc()
        finally:
            classFile.close()
            headersFile.close()
            filesNoProcessed.close()

        print("otherClases:",otherClases)



if __name__ == '__main__':
    TableClassificator().classify()




