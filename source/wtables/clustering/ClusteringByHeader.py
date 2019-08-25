import pandas as pd
from wtables.wikidata_db.WikidataDAO import *
import sys
import time

def getCluster0(fileAllTables, fileTablesRelations):
    """
    Generates a cluster by each table
    :param fileAllTables:  Contails every tables with their headers
    :param fileTablesRelations: Contains
    :return:
    """
    dfAllTables = pd.read_csv(fileAllTables, sep="\t",
                              dtype={"table": str}, index_col=False, compression='gzip')
    dfAllTables['cluster']=dfAllTables['table']
    print(dfAllTables.head(2))
    df = pd.read_csv(fileTablesRelations, sep="\t", names=["table", "cols", "relations"],
                     dtype={"table": str},
                     compression='gzip')

    df = df.join(dfAllTables.set_index("table"), on=["table"])
    print(df.head(2))
    dfna = df.dropna()
    print(dfna.head(2))
    dfPairRel = pd.DataFrame({"all_relations": dfna.groupby(['table', 'cols'])['relations'].
                             apply(lambda x: " ".join(x))}).reset_index()
    print(dfPairRel.head(2))
    dfPairRel["all_relations"] = dfPairRel["all_relations"].apply(lambda x: set(x.split(" ")))
    print(dfPairRel.head(2))
    df = pd.merge(dfPairRel, df, on=['table', 'cols'], how='left')
    df.to_csv("cluster0.csv", sep="\t", index=False, columns=["cluster", "table", "cols", "relations", "all_relations"])

def getRelationsByCluster(fileCluster):
    df = pd.read_csv(fileCluster, sep="\t", names=["cluster","table", "cols", "relations"], dtype={"table": str},
                     )
    dfPairRel = pd.DataFrame({"all_relations": df.dropna().groupby(['cluster', 'cols'])['relations'].
                             apply(lambda x: " ".join(x))}).reset_index()
    print(dfPairRel.head(2))
    dfPairRel["all_relations"] = dfPairRel["all_relations"].apply(lambda x: set(x.split(" ")))
    print(dfPairRel.head(2))
    df = pd.merge(dfPairRel, df, on=['cluster', 'cols'], how='left')
    df.to_csv('cluster2AllRelations.csv', sep='\t', index=False)
    print(df.head(10))

def getCluster1(fileAllTables, fileTablesRelations):
    dfAllTables = pd.read_csv(fileAllTables, sep="\t",
                              dtype={"table": str}, index_col=False,  compression='gzip')
    print(dfAllTables.head(2))
    df=pd.read_csv(fileTablesRelations, sep="\t", names=["table","cols","relations"], dtype={"table":str},
                   compression='gzip')

    df = df.join(dfAllTables.set_index("table"), on=["table"])
    print(df.head(2))
    dfna=df.dropna()
    print(dfna.head(2))
    dfPairRel=pd.DataFrame({"all_relations": dfna.groupby(['cluster', 'cols'])['relations'].
                           apply(lambda x: " ".join(x))}).reset_index()
    print(dfPairRel.head(2))
    dfPairRel["all_relations"] = dfPairRel["all_relations"].apply(lambda x: set(x.split(" ")))
    print(dfPairRel.head(2))
    df = pd.merge(dfPairRel, df, on=['cluster', 'cols'], how='left')
    df.to_csv("cluster1_1.csv", sep="\t", index=False, columns=["cluster","table", "cols", "relations", "all_relations"])


    #dfGeneral = pd.read_csv("generalCluster.csv", sep="\t", dtype={"table": str})
    #df = df[['cluster','table','count']].drop_duplicates()
    #df = df.rename(columns={'cluster': 'cluster1', 'count': 'tables_cluster1'})

    #dfGeneral=dfGeneral.join(df.set_index(["table"]), on=["table"])
    #dfGeneral.to_csv("generalCluster1.csv", sep="\t", index=False,
    #                 columns=['header','table','cluster0_1','tables_cluster0_1','cluster0_2','tables_cluster0_2','cluster1','tables_cluster1'])

def mergeCluster1_2(fileCluster1, fileCluster2):
    df_cluster1 = pd.read_csv(fileCluster1, sep="\t", dtype={"table": str})
    df_cluster1=df_cluster1[['table','cols','all_relations']]
    df_cluster1 = df_cluster1.rename(columns={'all_relations': 'relations_table'})
    df_cluster2 = pd.read_csv(fileCluster2, sep="\t", dtype={"table": str})
    df = pd.merge(df_cluster2, df_cluster1, on=['table','cols'], how='left')
    df.to_csv("clusterMerge0_1", sep="\t", index=False)


def getCluster2(fileCluster1, fileMaxRel):
    df_cluster1=pd.read_csv(fileCluster1, sep="\t",dtype={"table":str})
    df_cluster1 = df_cluster1[['cluster', 'table']].drop_duplicates()
    df_maxrel = pd.read_csv(fileMaxRel, names=["table", "cols", "max_rel", "score"],sep="\t", dtype={"table": str})
    cluster2 = pd.merge(df_maxrel, df_cluster1, on=['table'], how='left')
    cluster2=cluster2.dropna()
    cluster2=cluster2[["cluster", "table", "cols", "max_rel"]]
    cluster2 = cluster2.drop_duplicates()
    cluster2.to_csv("cluster2_max_rel.csv", sep="\t", index=False)

def getCluster2Merge(fileCluster1, fileCluster2):
    df_cluster1=pd.read_csv(fileCluster1, sep="\t",dtype={"table":str})
    df_cluster1=df_cluster1[df_cluster1['cluster'] != 'cluster']
    df_cluster1 = df_cluster1[['cluster','table','cols','relations']].rename(columns={'cluster': 'cluster1'})
    print(df_cluster1.head())
    df_cluster2 = pd.read_csv(fileCluster2, sep="\t", names=["cluster1", "cluster2", "table"],dtype={"table": str})
    dfCluster = pd.DataFrame({"count": df_cluster2.groupby(['cluster2']).size()}).reset_index()
    dfCluster = dfCluster.sort_values("count", ascending=False).reset_index(drop=True)
    df_cluster2 = df_cluster2.join(dfCluster.set_index("cluster2"), on=["cluster2"])
    df_cluster2 = df_cluster2.sort_values('cluster1')
    print(df_cluster2.head())

    df_cluster1['cluster1'] = df_cluster1['cluster1'].astype(int)
    df_cluster2['cluster1'] = df_cluster2['cluster1'].astype(int)
    cluster2 = df_cluster2.join(df_cluster1.set_index(['cluster1','table']), on=['cluster1','table'])
    #cluster2 = pd.merge(df_cluster1, df_cluster2, on=['cluster1','table'], how='left')
    print(cluster2.head(2))
    print(df_cluster1[df_cluster1['table']=='32162.24'])
    print(df_cluster2[df_cluster2['table'] == '32162.24'])
    print(cluster2[cluster2['table'] == '32162.24'])
    cluster2 = cluster2.rename(columns={'cluster2': 'cluster'})
    dfPairRel = pd.DataFrame({"all_relations": cluster2.dropna().groupby(['cluster', 'cols'])['relations'].apply(
        lambda x: " ".join(x))}).reset_index()
    dfPairRel["all_relations"] = dfPairRel["all_relations"].apply(lambda x: set(x.split(" ")))
    print(dfPairRel.head(2))
    cluster2 = pd.merge(dfPairRel, cluster2, on=['cluster', 'cols'], how='left')
    cluster2.to_csv("cluster2_2.csv", sep="\t", index=False, columns=["cluster","table", "cols", "relations", "all_relations"])

def clusteringPairRelation(fileConflicts, fileRels):
    fout = open("cluster2_1_nop.csv", "w")
    f1 = open(fileConflicts, "r")
    dictPairConflict = {}
    for line in f1.readlines():
        _line = line.replace("\n","").split("\t")
        #print(_line)
        if _line[2]=="null" or _line[3]=="null":
            continue
        cont = float(_line[4])
        val=max([(cont / float(_line[2])),(cont / float(_line[3]))])
        dictPairConflict[_line[0]]=float(_line[2])
        dictPairConflict[_line[1]]=float(_line[3])
        dictPairConflict[_line[0]+"#"+_line[1]] = val
    f1.close()
    df=pd.read_csv(fileRels, sep="\t", usecols=['cluster','table', 'cols','max_rel'],dtype={'table':str})

    print(df.head(5))
    setheaders=set(df['cluster'].tolist())
    dictSetHeadersCount={}
    print("Tables: ", len(setheaders))
    contset=0
    numcluster=0
    clusters={}
    for seth in setheaders:
        stt=time.time()
        print("Cont: ",contset)
        contset+=1
        df1=df[df['cluster']==seth]
        df1 = df1.fillna('$$')
        #print('Group size: ', df1.shape, seth)
        g1 = df1.groupby(['cols', 'max_rel'])['table'].apply(lambda x: "{'%s'}" % "', '".join(x))
        dictpairs = {}

        for (pair, rel), value in g1.iteritems():
            #print('rel ',rel, value)
            #if rel ==None:
            #    print('pair: ', pair, rel)
            if dictpairs.get(pair) == None:
                dictpairs[pair] = {}
            dictpairs[pair][rel] = set(eval(value))

        tempcluster={}
        noconf={}
        for col, rels in dictpairs.items():
            keyp = list(rels.keys())

            noconflict = []
            emp=[]
            for i in range(len(keyp)):
                for j in range(i + 1, len(keyp)):
                    pair = [keyp[i], keyp[j]]
                    pair.sort()

                    conf = dictPairConflict.get(
                        pair[0].replace("[", "").replace("]", "") + "#" + pair[1].replace("[", "").replace("]", ""))
                    if conf!=None:
                        if conf >= 0.5:
                            emp.extend(pair)
                            noconf[col+str(pair)] = conf
                            noconflict.append(pair)

            #print('keyp', keyp, 'noconflict', noconflict)
            indiv = set(keyp) - set(emp)
            for ind in indiv:
                tempcluster[col+str(ind)] = rels.get(ind)
                noconf[col+str(ind)] = 1
            #print("temp cluster", list(tempcluster.keys()))

            for pair in noconflict:

                tempcluster[col+str(pair)] = set()
                tab1 = rels.get(pair[0])

                tab2 = rels.get(pair[1])

                if tab1 is not None:
                    tempcluster[col + str(pair)]=tempcluster[col+str(pair)].union(tab1)
                if tab2 is not None:
                    tempcluster[col + str(pair)]=tempcluster[col+str(pair)].union(tab2)
        #print(tempcluster)
        noconf = sorted(noconf.items(), key=lambda kv: kv[1])
        #print(noconf)
        for nvi in range(len(noconf)):
                for nvj in range(nvi + 1, len(noconf)):
                    rel1k = noconf[nvi][0]
                    rel2k = noconf[nvj][0]
                    if noconf[nvi][1] == noconf[nvj][1]:
                        tab1 = tempcluster[noconf[nvi][0]]
                        tab2 = tempcluster[noconf[nvj][0]]
                        if len(tab2) >= len(tab1):
                            tempcluster[rel1k] = tempcluster[rel1k] - tempcluster[rel2k]
                        else:
                            tempcluster[rel2k] = tempcluster[rel2k] - tempcluster[rel1k]
                    else:
                        tempcluster[rel1k] = tempcluster[rel1k] - tempcluster[rel2k]
        for ktc, vtc in tempcluster.items():
            if len(vtc)>0:
                for tab in vtc:
                    fout.write(str(seth)+ "\t"+str(numcluster) + "\t" + tab + "\n")
                #clusters[str(seth)+"#"+str(numcluster)]=vtc
                numcluster+=1
    #for k, v in clusters.items():
    #    for tab in v:
    #        fout.write(k.replace("#","\t") + "\t" + tab +"\n")
    fout.close()

if __name__ == '__main__':
    args = sys.argv[1:]

    starttime=time.time()
    if args[0]=="0":
        getCluster0(args[1],args[2])
    if args[0]=="1":
        getCluster1(args[1], args[2])
    if args[0]=="2":
        getCluster2(args[1], args[2])
    if args[0]=="3":
        clusteringPairRelation(args[1], args[2])
    if args[0] == "4":
        getCluster2Merge(args[1], args[2])

    if args[0] == "5":
        mergeCluster1_2(args[1], args[2])

    print("Time execution: ", time.time()-starttime)
