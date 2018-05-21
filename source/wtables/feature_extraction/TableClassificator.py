# http://emunoz.org/publications/files/LD4IE2013.pdf
# Classification Tables: https://github.com/emir-munoz/wikitables

from bs4 import BeautifulSoup
import sys
import source.wtables.preprocessing.ReadHTML as readHTML
from os.path import basename
import bz2
import tarfile
import logging
import traceback


class TableClassificator(object):

    def classify(self):

        LOG_FILENAME = 'debug.log'
        args = sys.argv[1:]

        if (len(args) < 2):
            print("Use params <path> folder Zip files and <pathOutputFolder>")
            return
        pathTar = args[0]
        tmpFolder = "/tmp/wikitables"
        pathOutput = args[1];
        logging.basicConfig(filename=pathOutput+"/"+LOG_FILENAME, level=logging.DEBUG)
        classFile = bz2.BZ2File(pathOutput + "/classFile.out.bz2", "wb")
        headersFile = bz2.BZ2File(pathOutput + "/headersFile.out.bz2", "wb")
        try:
            tar = tarfile.open(pathTar)
        except:
            traceback.print_exc()
            return

        headersFile.write(
            ("fileName" + "\t" + "title" + "\t" + "headers" + "\n").encode("utf8"))
        classFile.write(
            ("fileName" + "\t" + "title" + "\t" + "totalTables" + "\t" + "wellFormedTables" + "\t"
             + "notWellFormedTables" + "\t" + "tocTables" + "\t" + "infoboxTables" + "\t"
                +"smallDimTables" + "\t"
             + "otherTables" + "\t" + "usefulTables" + "\t" + "wikiTables" + "\n").encode("utf8"))
        cont=1
        try:
            for member in tar.getnames():
                file = tar.extract(member, path=tmpFolder)
                logging.debug('Open File: '+str(cont)+" >"+member)
                bzFile = bz2.BZ2File(tmpFolder + "/" + member, "r")
                usefulTables = 0;
                wellFormedTables = 0;
                notWellFormedTables = 0;
                tocTables = 0;
                infoboxTables = 0;
                smallDimTables = 0;
                otherTables = 0;
                wikiTables = 0

                soup = BeautifulSoup(bzFile.read(), 'html.parser')
                title = readHTML.readTitle(soup)
                tables = readHTML.readTables(soup)
                for t in tables:
                    html, t2d = readHTML.tableTo2d(t)
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
                        or "metadata" in tclass
                        or "maptable" in tclass
                        or "vcard" in tclass):
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
                            stringHeaders = ""
                            for h in t2d.getHeadersText():
                                stringHeaders += h + ","
                            headersFile.write(
                                (basename(member) + "\t" + title + "\t" + stringHeaders + "\n").encode("utf8"))
                        continue

                    otherTables += 1

                classFile.write(
                    (basename(member) + "\t" + title + "\t" + str(len(tables)) + "\t" + str(wellFormedTables) + "\t"
                     + str(notWellFormedTables) + "\t" + str(tocTables) + "\t" + str(infoboxTables) + "\t" + str(
                        smallDimTables) + "\t"
                     + str(otherTables) + "\t" + str(usefulTables) + "\t" + str(wikiTables) + "\n").encode("utf8"))
                cont+=1
        except:
            traceback.print_exc()
        finally:
            classFile.close()
            headersFile.close()


if __name__ == '__main__':
    TableClassificator().classify()
