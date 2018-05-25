import pandas as pd
from itertools import islice
import linecache
import sys


class ClusterTables(object):
    def getMetric(self, epsilon, filein, fileOut):
        fileOut.write("article1" + "\t" + "table1" + "\t" + "article2" + "\t" + "table2" + "\t" + "metrica" + "\n")
        pas = 1
        totalLines = 1
        while pas <= totalLines:
            actualLine = linecache.getline(filein, pas + 1)
            if (actualLine == ""):
                break

            cont = 0
            with open(filein) as infile:
                for line in infile:
                    if cont == 0:
                        cont += 1
                        continue
                    iArticle = line.split("\t")[0]
                    iHeaders = line.split("\t")[3]

                    iHeaders = iHeaders.replace("\n", "").split(",")
                    iHeaders = [item.strip() for item in iHeaders]

                    actualArticle = actualLine.split("\t")[0]
                    actualHeaders = actualLine.split("\t")[3]
                    actualHeaders = actualHeaders.replace("\n", "").split(",")
                    actualHeaders = [item.strip() for item in actualHeaders]
                    intersect = set(actualHeaders).intersection(iHeaders)
                    metric = len(intersect) / (len(actualHeaders) + len(iHeaders))
                    if metric >= epsilon:
                        fileOut.write(
                            str(actualArticle) + "\t" + str(pas) + "\t" + str(iArticle) + "\t" + str(cont) + "\t" + str(
                                metric) + "\n")
                    cont += 1
                    if pas == 1:
                        totalLines += 1
                pas += 1
        fileOut.close()

    def dbscan(self, minPoints, fileData):
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
                neighbors.append(int(table))
                for row, data in group.iterrows():
                    table2 = data["table2"]
                    neighbors.append(int(table2))
                if len(neighbors) < minPoints:
                    continue
                else:
                    self.expandCluster(neighbors, g1, visited, minPoints, cluster, nodeCluster, indexCluster)
                    listClusters.append(cluster[:])
                indexCluster += 1

        return indexCluster, nodeCluster

    def expandCluster(self, neighbors, g1, visited, minPoints, cluster, nodeCluster, indexCluster):
        for n in neighbors:
            if visited.get(n) == None:
                visited[n] = 1
                try:
                    dfn = g1.get_group(int(n))
                except KeyError:
                    continue
                newNeighbors = dfn.ix[:, "table2"].tolist()
                newNeighbors = list(map(int, newNeighbors))
                if (len(newNeighbors) >= minPoints):
                    neighbors.extend(newNeighbors)
            if nodeCluster.get(n) == None:
                nodeCluster[n] = indexCluster
                cluster.append(n)


if __name__ == '__main__':
    test = ClusterTables()
    LOG_FILENAME = 'debug.log'
    args = sys.argv[1:]

    if (len(args) != 3):
        print(
            "Use params <fileName_headers>.csv,  <fileName_scores>.csv and option: [1=get scores or 2=clustering or 3=both]")
    else:
        filein = args[0]  # "/home/jhomara/Desktop/headersFile.csv"
        scores = args[1]  # "/home/jhomara/Desktop/decompress/scores.csv"
        option = args[2]
        if option == "1":
            fileout = open(scores, "w")
            test.getMetric(0.4, filein, fileout)
        elif option == "2":
            numCluster, listCluster = test.dbscan(10, scores)
            print("numCluster", numCluster)
        elif option == "3":
            fileout = open(scores, "w")
            test.getMetric(0.4, filein, fileout)
            numCluster, listCluster = test.dbscan(10, scores)
            print("numCluster", numCluster)
        else:
            print("Option not valid!. Try: [0=get scores or 1=clustering or 2=both]")
