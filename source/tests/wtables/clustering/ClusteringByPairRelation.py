import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import time
import ast
import json
def clustering():
    fout = open("/home/jhomara/Desktop/web7/clusters.csv", "w")
    fdict = open("/home/jhomara/Desktop/web7/propertiesVirtuoso.out", "r")
    dictProp = {}
    for line in fdict.readlines():
        _line = line.replace("\n", "").split("\t")
        dictProp[_line[0]] = _line[1]
    fdict.close()
    f1 = open("/home/jhomara/Desktop/web7/conflictRelations.csv", "r")
    dictPair = {}
    dictPairMax = {}
    for line in f1.readlines():
        _line = line.replace("\n","").split("\t")
        #print(_line)
        if _line[2]=="null" or _line[3]=="null":
            continue
        cont = float(_line[4])
        val=max([(cont / float(_line[2])),(cont / float(_line[3]))])
        # dictPair[_line[0]]=val
        dictPairMax[_line[0]]=float(_line[2])
        dictPairMax[_line[1]]=float(_line[3])
        dictPairMax[_line[0]+"#"+_line[1]] = val
    f1.close()
    df=pd.read_csv("/home/jhomara/Desktop/web7/tablesMaxRel.csv", sep="\t", usecols=['table', 'cols','max_rel','index'],dtype={'table':str})
    setheaders=set(df['index'].tolist())
    dictSetHeadersCount={}
    print("Tables: ", len(setheaders))
    contset=0
    for seth in setheaders:
        print("Cont: ",contset)
        df1=df[df['index']==seth]
        print('Group size: ', df1.shape, seth)
        g1 = df1.groupby(['cols', 'max_rel'])['table'].apply(lambda x: "{'%s'}" % "', '".join(x))
        start_time = time.time()
        dictpairs = {}
        cont = 0
        for (pair, rel), value in g1.iteritems():
            if dictpairs.get(pair) == None:
                dictpairs[pair] = {}
            dictpairs[pair][rel] = set(eval(value))
        dictTables = {}
        for col, rels in dictpairs.items():
            keyp = list(rels.keys())
            for k, tab in rels.items():
                tables = list(tab)
                tables.sort()
                for i in range(len(tables)):
                    for j in range(i, len(tables)):
                        t1 = tables[i]
                        t2 = tables[j]
                        val = dictTables.get(t1)
                        if val == None:
                            dictTables[t1] = {}
                            dictTables[t1][t2] = 1
                        else:
                            if val.get(t2) == None:
                                dictTables[t1][t2] = 1
                            else:
                                minr = min([val.get(t2), 1])
                                dictTables[t1][t2] = minr

                        val = dictTables.get(t2)
                        if val == None:
                            dictTables[t2] = {}
                            dictTables[t2][t1] = 1
                        else:
                            if val.get(t1) == None:
                                dictTables[t2][t1] = 1
                            else:
                                minr = min([val.get(t1), 1])
                                dictTables[t2][t1] = minr
            pairsk = []
            for i in range(len(keyp)):
                for j in range(i + 1, len(keyp)):
                    pair = [keyp[i], keyp[j]]
                    pair.sort()
                    pairsk.append(pair)
            for pair in pairsk:
                tables1 = rels.get(pair[0])
                tables2 = rels.get(pair[1])
                conf = dictPairMax.get(
                    pair[0].replace("[", "").replace("]", "") + "#" + pair[1].replace("[", "").replace("]", ""))
                if conf == None:
                    conf = 0
                for t1 in tables1:
                    for t2 in tables2:
                        val = dictTables.get(t1)
                        if val == None:
                            dictTables[t1] = {}
                            dictTables[t1][t2] = conf
                        else:
                            if val.get(t2) == None:
                                dictTables[t1][t2] = conf
                            else:
                                minr = min([val.get(t2), conf])
                                dictTables[t1][t2] = minr

                        val = dictTables.get(t2)
                        if val == None:
                            dictTables[t2] = {}
                            dictTables[t2][t1] = conf
                        else:
                            if val.get(t1) == None:
                                dictTables[t2][t1] = conf
                            else:
                                minr = min([val.get(t1), conf])
                                dictTables[t2][t1] = minr
        #print("--- %s seconds ---" % (time.time() - start_time))

        alltables = set(df1.dropna()['table'].tolist())
        alltables = list(alltables)
        alltables.sort()
        start_time = time.time()
        cl = 0
        clusters = {}
        if len(alltables)==0:
            clusters[cl] = []
            print("Nan table")
        else:
            clusters[cl] = [alltables[0]]
            for i in range(1, len(alltables)):
                clusterExist = False
                listcl = {}
                for k, v in clusters.items():
                    cont = 0
                    for t in v:
                        if dictTables.get(alltables[i])==None:
                            continue
                        if dictTables.get(alltables[i]).get(t) == None:
                            # print("None")
                            cont += 1
                        else:
                            if dictTables.get(alltables[i]).get(t) >= 0.5:
                                cont += 1

                    if cont == len(v):
                        listcl[k] = len(v)
                listcl = sorted(listcl.items(), key=lambda kv: kv[1])
                if len(listcl) > 0:
                    lastcl = listcl[len(listcl) - 1]
                    clusters[lastcl[0]].append(alltables[i])
                else:
                    cl += 1
                    clusters[cl] = [alltables[i]]
        #print("--- %s seconds ---" % (time.time() - start_time))
        contset+=1
        clustersCount = {}
        for k, v in clusters.items():
            clustersCount[k] = len(v)
        for k, v in clustersCount.items():
            fout.write(str(seth)+"\t"+str(k)+"\t"+str(v)+"\n")
    fout.close()

def plotClusters(file):
    fout=open("/home/jhomara/Desktop/web7/clusters_result.csv", "w")
    with open(file, "r") as data_file:
        #data = json.load(data_file)
        json_data = ast.literal_eval(data_file.read())
        #data = json.loads(json_data)
        #data=eval(data)
        print(type(json_data))
        print(len(json_data.keys()))
        dictc={}
        for k, v in json_data.items():
            #print(v)
            dictc[k]={}
            for kv, v1 in v.items():
                dictc[k][kv]=len(v1)
        for k, v in dictc.items():
            clusters = sorted(v.items(), key=lambda kv: kv[1])
            lastcl=clusters[len(clusters)-1][1]
            fout.write(str(k)+"\t"+str(len(clusters))+"\t"+str(lastcl)+"\n")

#plotClusters("/home/jhomara/Desktop/web7/clusters.json")

clustering()
