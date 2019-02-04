
import pandas as pd
from io import StringIO
import gzip
import sys
import logging

def scoreRelations(fileIn):
    linesFile=""
    linesFile2 = ""
    fileOut="scoreRelationsJsonUpdate2.out.gz"
    fileOut2 = "scoreRelationsByTable2.out.gz"
    aux=""
    cont=0
    with gzip.open(fileOut, "wt") as fileScoreTable:
        with gzip.open(fileOut2, "wt") as fileScoreRow:
            with gzip.open(fileIn, "rt") as fi:
                for line in fi:
                    _line=line.replace("\n","").split("\t")
                    tableId=_line[0]
                    rows=_line[4]
                    col1= _line[5]
                    col2 = _line[6]
                    link1 = _line[7]
                    link2 = _line[8]
                    wd1=_line[9]
                    wd2=_line[10]
                    if wd1=="" or wd2=="":
                        continue
                    prop = _line[11]
                    cols=col1+"##"+col2
                    if aux!=tableId and aux!="":
                        logging.debug('Line: ' + str(cont))
                        out=writeDF(linesFile)
                        out2=writePropsByRow(linesFile2)
                        fileScoreTable.write(out)
                        fileScoreRow.write(out2)
                        linesFile=""
                        linesFile2=""
                        cont+=1
                    aux=tableId
                    linesFile+=tableId+"\t"+cols+"\t"+prop+"\n"
                    linesFile2 += tableId + "\t"+ rows +"\t"+ cols + "\t" + link1+"\t"+link2+"\t"+wd1+"\t"+wd2+"\t"+prop + "\n"


                out = writeDF(linesFile)
                out2 = writePropsByRow(linesFile2)
                fileScoreTable.write(out)
                fileScoreRow.write(out2)
    fileScoreRow.close()
    fileScoreTable.close()
def writeDF(linesFile):
    data = StringIO(linesFile)
    df = pd.read_csv(data, sep="\t", names=['table', 'cols', 'prop'], dtype={'table': str})
    dfcols = df[['table', 'cols']].drop_duplicates()
    dfprop = df[df['prop'].notnull()]
    # #print(df.head(2))
    if dfprop.shape[0] > 0:
        cp = pd.DataFrame({'pcount': dfprop.groupby(['table', 'cols', 'prop']).size()}).reset_index()
        # #print(cp.head(2))

        cp['pcount'] = cp['prop'] + ":" + cp['pcount'].astype(str)
        dfPairRel = pd.DataFrame({"relations": cp.groupby(['table', 'cols'])['prop'].apply(
            lambda x: " ".join(x))}).reset_index()
        dfPairRelScore = pd.DataFrame({"relations_score": cp.groupby(['table', 'cols'])['pcount'].apply(
            lambda x: " ".join(x))}).reset_index()
        #print(dfcols.head(2))
        #print(dfPairRel.head(2))
        dfcols = pd.merge(dfcols, dfPairRel, on=['table', 'cols'])
        #print(dfcols.head(2))
        dfcols = pd.merge(dfcols, dfPairRelScore, on=['table', 'cols'])
    else:
        dfcols['relations'] = ''
        dfcols['relations_score'] = ''
    # #print(dfcols.head(2))
    s = StringIO()
    dfcols.to_csv(s, index=False, header=False, sep="\t")
    return s.getvalue()

def writePropsByRow(linesFile):
    data = StringIO(linesFile)
    df = pd.read_csv(data, sep="\t", names=['table','rows','cols','link1','link2','wd1','wd2','prop'], dtype={'table': str})
    df0 = df[df['prop'].isnull()]
    df0['relations']=df0['prop']
    print(df0)
    df=df.dropna()
    df=pd.DataFrame({'relations':df.groupby(['table','rows','cols','link1','link2','wd1','wd2'])['prop'].apply( lambda x: ",".join(x))}).reset_index()
    df=df.append(df0)
    #print(df.head())
    s = StringIO()
    df.to_csv(s, index=False, header=False, sep="\t")
    return s.getvalue()



if __name__ == '__main__':

    args = sys.argv[1:]
    logging.basicConfig(filename="./debug.log", level=logging.DEBUG)
    scoreRelations("testLinks.out.gz")

#df = pd.DataFrame([list(range(2))], columns=['1','2'])
#df.to_csv(s, index=False, header=False, sep="\t")
##print(s.getvalue())


