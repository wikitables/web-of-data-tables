import pandas as pd
import sys
from wtables.preprocessing.TextProcessing import TextProcessing
import gzip

def filterHeaders(fileIn):

    filteredTables =gzip.open ("filteredTables.csv", "wt")
    sortedHeaderTables = gzip.open("sortedHeaderTables.csv.gz", "wt")
    fileOneHeader = gzip.open("oneHeaderTables.csv.gz", "wt")
    errorTables = gzip.open("errorTables.csv.gz", "wt")
    cont=0
    textProcessing=TextProcessing()
    with gzip.open(fileIn, "rt") as fi:
        for line in fi:
            print("Line: ", cont)
            cont+=1
            _line=line.replace("\n","").split("\t")
            tableId=_line[0]
            numtable=tableId.split(".")[1]
            headers=_line[1]
            headers = eval(headers.replace('\"\"', ''))
            ncols = _line[2]
            nrows = _line[3]
            start = _line[4]
            types=["@"+h.split("@")[1] for h in header
            head1 = [h.split("@")[0] for h in headers]
            #head1=[textProcessing.cleanCellHeader(h) for h in head1]
            removeEndSpan = [h[:-9] if "**" in h and h.endswith("spancol") else h for h in head1]
            headers=[]
            for i in range(len(head1)):
                headers.append(removeEndSpan[i]+types[i])

            noSorted=headers[:]
            headers1 = [h for h in headers if "@4" not in h]
            nospancol=[h for h in headers1 if "spancol" not in h]
            if len(set(nospancol))==0:
                print(nospancol)
                print("Table span: "+ tableId, headers)
                errorTables.write(line)
                continue
            headers1.sort()
            if len(nospancol)==1:
                fileOneHeader.write(tableId+"\t"+str(headers1)+"\t"+ncols+"\t"+nrows+"\t"+start+"\n")
            else:
                sortedHeaderTables.write(tableId + "\t" + str(headers1) + "\t" + ncols + "\t" + nrows + "\t" + start + "\n")
            filteredTables.write(tableId + "\t" + str(noSorted) + "\t" + ncols + "\t" + nrows + "\t" + start + "\n")
        fileOneHeader.close()
        filteredTables.close()
        sortedHeaderTables.close()
        errorTables.close()

def clusterTablesByHeader(fileIn, fileOneHeader):
    df=pd.read_csv(fileIn, sep="\t", names=['table','headers','ncols','nrows','start'],dtype={'table':str}, compression='gzip')
    dfg=pd.DataFrame({'count':df.groupby('headers').size()}).reset_index()
    dfg=dfg.sort_values('count', ascending=False).reset_index(drop=True)
    dfg=dfg.reset_index()
    dfg=dfg.rename(columns={'index': 'cluster'})
    df=pd.merge(df, dfg, on="headers")
    df=df[['cluster','table','headers','ncols', 'nrows', 'count']]

    df1 = pd.read_csv(fileOneHeader, sep="\t", names=['table', 'headers','ncols', 'nrows', 'start'], dtype={'table': str},compression='gzip')
    df1 = df1.reset_index(drop=True).reset_index()
    df1 = df1.rename(columns={'index': 'cluster'})
    df1['count']=1
    print(df.sort_values('cluster', ascending=False)[['table','cluster']].head(2))
    lastCluster=dfg.shape[0]
    print("Last cluster: ", lastCluster)
    df1['cluster']=df1['cluster']+lastCluster
    df1 = df1[['cluster','table', 'headers', 'ncols', 'nrows','count']]
    df=df.append(df1)
    df =df.sort_values("cluster", ascending=True)
    print(df.head(50))
    df.to_csv("cluster1.csv", index=False, sep="\t" )

if __name__=="__main__":
    args=sys.argv[1:]

    if args[0]=='1':
        filterHeaders(args[1])
    else:
        if args[0]=="2":
            clusterTablesByHeader(args[1], args[2])