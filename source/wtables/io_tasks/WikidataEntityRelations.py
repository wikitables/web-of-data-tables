import gzip
import sys

def fillRelations(fileTableRelations):
    with gzip.open(fileTableRelations,"rt") as fin:
        for line in fin:
            split=line.replace("\n","").split("\t")
            if len(split)<=1:
                continue
            wd1=split[0]
            wd2 = split[1]
            if wd1!=None and wd1!="" and wd2!=None and wd2!="":
                obj=DICT_RELATIONS.get(wd1)
                if obj==None:
                    DICT_RELATIONS[wd1]={wd2: []}
                else:
                    DICT_RELATIONS[wd1][wd2] = []
                obj = DICT_RELATIONS.get(wd2)
                if obj == None:
                    DICT_RELATIONS[wd2] = {wd1: []}
                else:
                    DICT_RELATIONS[wd2][wd1] = []


def readWikidataFile(wikidataFile):
    fout=open("wikidata-relations.txt","w")
    with gzip.open(wikidataFile,"rt") as fin:
        for line in fin:
            split = line.replace("\n","").split("\t")
            wd1 = split[0]
            prop = split[1]
            wd2 = split[2]
            objs=DICT_RELATIONS.get(wd1)
            if objs!=None:
                if objs.get(wd2)!=None:
                    fout.write(wd1+"\t"+prop+"\t"+wd2+"\n")
                else:
                    print("No exist: ", wd1, wd2)
            else:
                print("No exist: ", wd1, wd2)
    fout.close()
if __name__ == '__main__':
    args = sys.argv[1:]
    fileTableRelations=args[0]
    fileWikidata=args[1]
    DICT_RELATIONS={}
    fillRelations(fileTableRelations)
    readWikidataFile(fileWikidata)