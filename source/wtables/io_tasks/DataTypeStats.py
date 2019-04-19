import pandas as pd

def readTableDataTypes(fileData):
    with open("outTableTypes.csv", "w") as fout:
        with open(fileData, "r") as fin:
            for line in fin:
                _line=line.replace("\n","").split("\t")
                table=_line[0]
                headers = eval(_line[1])
                if len(headers)==0:
                    continue
                hnospan = [h for h in headers if "spancol" not in h]
                if len(hnospan)==0:
                    continue
                dtable={1:0,2:0,3:0,4:0}
                for h in headers:
                    type=int(h.split("@")[1])
                    dtable[type]+=1
                fout.write(table+"\t"+str(dtable.get(1))+"\t"+
                           str(dtable.get(2))+"\t"+str(dtable.get(3))+"\t"+
                           str(dtable.get(4))+"\n")

def readResults(file1, fileCluster):
    df1=pd.read_csv(file1, sep='\t', names=['table','1','2','3','4'], dtype={'table':str}, index_col=False)
    print(df1.head())
    df2=pd.read_csv(fileCluster, sep="\t", index_col=False, dtype={'table':str})
    df1=df1.join(df2.set_index('table'), on='table')
    print(df2.head())
    print(df1.head())
    print(df1[df1['cluster'].isnull()])

#readTableDataTypes("/home/jhomara/Desktop/infoTablesV2.csv")
readResults('outTableTypes.csv', '/home/jhomara/Desktop/cluster.csv')
