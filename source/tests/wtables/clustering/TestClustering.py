# -*- coding: utf-8 -*-
# import linecache
import sys
import time

import pandas as pd
# from multiprocessing import Manager, Array
# import multiprocessing
# from joblib import Parallel, delayed
import tempfile, os
# from joblib import dump, load
# import numpy as np
# import shutil
from pathlib import Path
import traceback
from itertools import islice
import io


class ClusterTables(object):

    def getMetric1(self, filein, fileOut, epsilon=0.2):
        fileOut = open(fileOut, "w")
        fileOut.write("table1" + "\t" + "table2" + "\t" + "score" + "\n")
        numLines = 0
        pas = 0
        while pas <= numLines:
            firstLine = []
            firstLineName = ""
            if pas == 0:
                with open(filein) as infile:
                    for line in infile:
                        actualLine = line.replace("\n", "")
                        actualLine1 = actualLine.replace(",", " ")
                        actualHeaders = actualLine1.split("\t")[1]
                        actualHeaders = actualHeaders.split()
                        nextLine = []
                        if numLines == 0:
                            firstLine.extend(actualHeaders)
                            firstLineName = line.replace("\n", "")
                        else:
                            nextLineName = line.replace("\n", "")
                            nextLine.extend(actualHeaders)
                            intersect = set(firstLine).intersection(set(nextLine))
                            metric = len(intersect) / (len(set(firstLine)) + len(set(nextLine)))
                            if metric >= epsilon:
                                fileOut.write("(" + str(firstLineName).replace("\t", "[") + "])" + "\t" + "(" + str(
                                    nextLineName).replace("\t", "[") + "])" + "\t" + str(metric) + "\n")
                                fileOut.write("(" + str(nextLineName).replace("\t", "[") + "])" + "\t" + "(" + str(
                                    firstLineName).replace("\t", "[") + "])" + "\t" + str(metric) + "\n")
                        numLines += 1
            else:
                cont = 0
                with open(filein) as infile:
                    print("pas:", pas, numLines)
                    for line in islice(infile, pas, numLines):
                        actualLine = line.replace("\n", "")
                        actualLine1 = actualLine.replace(",", " ")
                        actualHeaders = actualLine1.split("\t")[1]
                        actualHeaders = actualHeaders.split()
                        nextLine = []
                        if cont == 0:
                            firstLine.extend(actualHeaders)
                            firstLineName = line.replace("\n", "")
                        else:
                            nextLineName = line.replace("\n", "")
                            nextLine.extend(actualHeaders)
                            intersect = set(firstLine).intersection(set(nextLine))
                            metric = len(intersect) / (len(set(firstLine)) + len(set(nextLine)))
                            if metric >= epsilon:
                                fileOut.write("(" + str(firstLineName).replace("\t", "[") + "])" + "\t" + "(" + str(
                                    nextLineName).replace("\t", "[") + "])" + "\t" + str(metric) + "\n")
                                fileOut.write("(" + str(nextLineName).replace("\t", "[") + "])" + "\t" + "(" + str(
                                    firstLineName).replace("\t", "[") + "])" + "\t" + str(metric) + "\n")
                        cont += 1
            pas += 1

        fileOut.close()

    def getMetric(self, i, actualLine, allLines, queue, epsilon=0.2):
        actualLine = actualLine.replace("\n", "")
        actualLine1 = actualLine.replace(",", " ")
        actualHeaders = actualLine1.split("\t")[1]
        actualHeaders = actualHeaders.split()

        print("[Worker %d], %d" % (os.getpid(), i))
        for enu, line in enumerate(allLines):
            line = line.replace("\n", "")
            line1 = line.replace(",", " ")
            iHeaders = line1.split("\t")[1]
            iHeaders = iHeaders.split()
            intersect = set(actualHeaders).intersection(set(iHeaders))
            metric = len(intersect) / (len(set(iHeaders)) + len(set(actualHeaders)))
            if metric >= epsilon:
                queue.put("(" + str(actualLine).replace("\t", "[") + "])" + "\t" + "(" + str(line).replace("\t",
                                                                                                           "[") + "])" + "\t" + str(
                    metric) + "\n")

    def dbscan(self, minPoints, fileData, epsilon=0.2):
        df = pd.read_csv(fileData, sep="\t")
        visited = {}
        nodeCluster = {}
        neighbors = []
        g1 = df.groupby('table1')
        cluster = []
        listClusters = []
        indexCluster = 0
        for table, group in g1:
            del cluster[:]
            if visited.get(table) == None:
                visited[table] = 1
                del neighbors[:]
                neighbors.append(table)
                groupf = group[group["score"] >= epsilon]
                for row, data in groupf.iterrows():
                    table2 = data["table2"]
                    neighbors.append(table2)
                if len(neighbors) < minPoints:
                    continue
                else:
                    self.expandCluster(neighbors, g1, visited, minPoints, cluster, nodeCluster, indexCluster, epsilon)
                    listClusters.append(cluster[:])
                indexCluster += 1

        return indexCluster, nodeCluster

    def expandCluster(self, neighbors, g1, visited, minPoints, cluster, nodeCluster, indexCluster, epsilon):
        for n in neighbors:
            if visited.get(n) == None:
                visited[n] = 1
                try:
                    dfn = g1.get_group(n)
                except KeyError:
                    continue
                newNeighborsf = dfn[dfn["score"] >= epsilon]
                newNeighbors = newNeighborsf.ix[:, "table2"].tolist()
                newNeighbors = list(map(str, newNeighbors))
                if (len(newNeighbors) >= minPoints):
                    neighbors.extend(newNeighbors)
            if nodeCluster.get(n) == None:
                nodeCluster[n] = indexCluster
                cluster.append(n)


def Writer(dest_filename, some_queue, some_stop_token):
    with open(dest_filename, 'w') as dest_file:
        while True:
            line = some_queue.get()
            if line == some_stop_token:
                return
            dest_file.write(line)


if __name__ == '__main__':
    start_time = time.time()
    test = ClusterTables()
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) != 1):
        print(
            "Use params <folder>. Folder has to contain headersFile.out")
    else:
        folderWork = args[0]  # "/home/jhomara/Desktop/headersFile.csv"
        scores = args[0] + "/headersCount.csv"  # "/home/jhomara/Desktop/decompress/scores.csv"

        # fileout = open(scores, "w")
        # try:
        # manager = Manager()
        # tmp = tempfile.mkdtemp()
        # listTabletmp = os.path.join(tmp, 'listTabletmp')
        try:
            # if option == "1":
            # test.getMetric1(folderWork + "/headersFile.out", scores)
            test.countHeaders(folderWork + "/headersFile.out", scores)
            # dump(open(folderWork + "/headersFile.out", "r").readlines(), listTabletmp)
            # listTables = load(listTabletmp, mmap_mode='r')
            # m = multiprocessing.Manager()
            # queue = m.Queue()
            # queue.put("table1" + "\t" + "table2" + "\t" + "score"+"\n")
            # Parallel(n_jobs=4)(delayed(test.getMetric)(i,line, listTables, queue)
            #               for i, line in enumerate(listTables))
            # queue.put("STOP")
            # writer_process = multiprocessing.Process(target=Writer, args=(scores, queue, "STOP"))
            # writer_process.start()
            # writer_process.join()
            # else:
            # if option == "2":
            #    scoresFile = Path(scores)
            #    fout = open(folderWork + "/clusters.csv", "w")
            #    fout.write("cluster" + "\t" + "item" + "\n")
            #    if scoresFile.exists():
            #        numCluster, listCluster = test.dbscan(2, scores)
            #        for key, value in listCluster.items():
            #            fout.write(str(value) + "\t" + str(key) + "\n")
            #        fout.close()
            #    else:
            #        print("Folder %s doesn't contain scores.csv file" % (folderWork))
            # else:
            #    print("Option not valid. Use 1=scores or 2=clustering")
        except IOError:
            traceback.print_exc()
            print("Folder %s doesn't contain headersFile.out file" % (folderWork))
        except Exception:
            traceback.print_exc()
            # finally:
            #    traceback.print_exc()
            # try:
            #    shutil.rmtree(tmp)
            # except:
            # print("Failed to delete: " + tmp)
