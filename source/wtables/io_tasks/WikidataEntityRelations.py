import gzip
import sys

def fillRelations(fileTableRelations):
    with gzip.open(fileTableRelations,"rt") as fin:
        for line in fin:
            split=line.replace("\n","").split(" ")
            if len(split)<=1:
                continue
            wd1=split[0]
            wd2 = split[1]
            obj=DICT_RELATIONS.get(wd1)
            if obj==None:
                DICT_RELATIONS[wd1]={wd2: 1}
            else:
                DICT_RELATIONS[wd1][wd2] = 1
            obj = DICT_RELATIONS.get(wd2)
            if obj == None:
                DICT_RELATIONS[wd2] = {wd1: 1}
            else:
                DICT_RELATIONS[wd2][wd1] = 1


def readWikidataFile(wikidataFile):
    cont=0
    with gzip.open("wikidataRels.txt.gz","wt") as fout:
        with gzip.open(wikidataFile,"rt") as fin:
            for line in fin:
                print("Line: ", cont)
                cont+=1
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