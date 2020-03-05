import numpy as np
from wtables.schema.Article import *
from wtables.utils.ParseLink import *
import pandas as pd
from wtables.preprocessing.TextProcessing import TextProcessing
from pathlib import Path

from wtables.wikidata_db.ConfigProperties import ConfigProperties


def unique(a):
    b = a.T
    diff = np.diff(b, axis=0)
    ui = np.ones(len(b), 'bool')
    ui[1:] = (diff != 0).any(axis=1)
    return b[ui].T

def cleanCell(text):
    whitelist = ['a', 'abbr', 'img']
    soupTag=BeautifulSoup(text, "html.parser")
    innerTags = soupTag.find_all()
    for tag in innerTags:
        if tag.name not in whitelist:
            tag.attrs = {}
    return str(soupTag)

def buildBigTable(cluster,tablesCluster):
        totalRows = 0
        print("filling cluster", cluster)
        fileEx = Path(CLUSTER_FILES + "/cluster_" + str(cluster) + ".json")
        if fileEx.is_file():
            print("Already", cluster)
            return
    #try:   fileEx
        textProcessing=TextProcessing()
        bigHeader = {}
        bigMatrix=[]
        print("Cluster: ", cluster, str(tablesCluster.shape))
        for c, t in tablesCluster.iterrows():
            jsonFile = t['table'].split(".")[0]
            file = open(FOLDER_JSON_FILES + "/" + jsonFile + ".json", "r")
            obj = file.read()
            obj = json.loads(obj)
            article = ComplexDecoder().default(obj)
            linkArticle = wikiLink(article.title)

            for table in article.tables:
                if table.tableId == t['table']:
                    tarray = np.array(table.htmlMatrix)
                    colHeaders=[h.split("@")[0] for h in table.colHeaders if "spancol" not in h]
                    colHeaders = [textProcessing.stemWord(" ".join(hi.split(" "))) for hi in colHeaders if hi != ""]
                    colH=["TITLE","TABLE"]
                    for h in colHeaders:
                        colH.append(h)
                    titleCell = np.array([['<td><a href="{}">{}</a></td>'.format(linkArticle, article.title)]
                                        for x    in range(len(tarray))])
                    tableCell = np.array([['<td>{}</td>'.format(table.tableId)]
                                          for x  in range(len(tarray))])
                    tarray=np.insert(tarray,[0], titleCell, axis=1)
                    tarray = np.insert(tarray,[1], tableCell, axis=1)

                    totalRows += table.nrows

                    dicth={}
                    cols=[]
                    for i, h in enumerate(colH):
                        if h not in cols:
                            cols.append(h)
                        dicth[h]=i
                    pos=list(dicth.values())
                    pos.sort()
                    pos=np.array(pos)
                    tarray=tarray[:, pos]
                    if len(bigHeader)==0:
                        for i, h in enumerate(cols):
                            bigHeader[h]=i
                    orderCol=[]
                    for h in cols:
                        orderCol.append(bigHeader.get(h))
                    tarray = tarray[:, np.array(orderCol)]
                    if len(bigMatrix)==0:
                        bigMatrix=tarray[table.startRows:,:]
                    else:
                        bigMatrix=np.concatenate((bigMatrix, tarray[table.startRows:,:]))

        bigHeader= sorted(bigHeader.items(), key=lambda kv: kv[1])
        bigH=[]
        for value in bigHeader:
            bigH.append(value[0])


        headerMatrix = np.array([["<th>{}</th>".format(h) for h in bigH]])
        bigMatrix=np.concatenate((headerMatrix, bigMatrix))
        bigTable2D = Table(title="1", attrs={}, htmlMatrix=bigMatrix.tolist(), colHeaders=bigHeader, rowHeaders=[], startRows=1,
                           nrows=totalRows, ncols=len(bigHeader), html="")
        articleCluster = Article(articleId=str(cluster), title=str(cluster), tables=[bigTable2D])
        f = open(CLUSTER_FILES + "/cluster_" + str(cluster) + ".json", "w")
        f.write(json.dumps(articleCluster.reprJSON(), cls=ComplexEncoder, skipkeys=True))
        f.close()

    #except Exception:
    #    traceback.print_exc()
    #    print("Error cluster", cluster)



def buildTableMatrix(tablesCl, header, totalRows):
    tableHtml = [[""] * len(header) for row in range(totalRows + 1)]
    tableHtml[0] = ["<th>{}</th>".format(h) for h in header]
    rowt = 1
    nextRow=1
    for table in tablesCl:
        for i, h in enumerate(header):
            aux=rowt
            rowt = nextRow
            v=table.get(h)
            if v!=None:
                rows = len(v)
                for row in range(0, rows):
                    tableHtml[rowt][i] = v[row]
                    rowt+=1
            else:
                print("No header: ", h)
                rowt=aux
        nextRow=rowt


    bigTable2D = Table(title="1", attrs={},htmlMatrix=tableHtml, colHeaders=header, rowHeaders=[], startRows=1, nrows=totalRows, ncols=len(header), html="")
    return bigTable2D


def mergeTablesCluster(fileCluster):

    print("Reading file")
    df = pd.read_csv(fileCluster, sep="\t", dtype={'table': str})
    print("Grouping cluster")
    clusters = df.groupby('cluster')
    print("Build super table")
    for cl, tablesCl in clusters:
        buildBigTable(cl, tablesCl)
    #Parallel(n_jobs=8)(delayed(buildBigTable)(cl, tablesCl) for cl, tablesCl in clusters)


if __name__ == '__main__':
    args = sys.argv[1:]
    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = params.get("json_files")
    # "/home/jhomara/Desktop/web7/json" json base folder
    CLUSTER_FILES = args[0]  #  "/home/jhomara/Desktop/web7/jsonClusterFolder" #folder wich will to keep html cluster files
    clusterFile =args[1]  # "test" #cluster file [cluster, table] .csv
    mergeTablesCluster(clusterFile)
