import sys, os
import logging
from joblib import Parallel, delayed
import traceback
from wtables.preprocessing.JsonArticle import *
from multiprocessing import Manager
import json
import re


class FeaturesExtractor(object):
    def extractHeaders(self, file, dict):
        f = file.split(".")
        if (f[1] == "json"):
            obj = json.loads(open(file).read())
            decoder = ComplexDecoder()
            art = decoder.default(obj)
            for t in art.tables:
                tid = t.tableId
                theaders=t.getHeadersText()
                if len(theaders)>0:
                    headrs = [re.sub('\W+', " ", h) for h in t.getHeadersText()]
                    dict[tid] = ",".join(headrs)




if __name__ == '__main__':
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) != 2):
        print("Use params <path> folder Json files and <pathOutputFolder>")
    else:
        manager = Manager()
        fe=FeaturesExtractor()
        pathZip = args[0]
        pathOutput = args[1]

        headersFile = open(pathOutput + "/headersFile.out", "wb")

        headersDict = manager.dict()
        logging.basicConfig(filename=pathOutput + "/" + LOG_FILENAME, level=logging.DEBUG)

        try:
            for subdir, dirs, files in os.walk(pathZip):
                Parallel(n_jobs=4, backend="multiprocessing")(
                    delayed(fe.extractHeaders)((pathZip + "/" + fileName), headersDict) for fileName in files)

            for key, value in sorted(headersDict.items()):
                headersFile.write((str(key) + '\t' + str(value) + '\n').encode("utf8"))

        except:
            traceback.print_exc()
