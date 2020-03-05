import gzip
import sys

def filterTriples(fileTriples, fileWikidataIdLinks):
    dictWikidata={}
    with open(fileWikidataIdLinks, "r") as fin:
        for line in fin:
            _line=line.replace("\n","").split("\t")
            dictWikidata[_line[1]]=1

        with gzip.open('wikidataTriplesFilteredSubj.txt.gz', "wt") as fout:
            with gzip.open(fileTriples, "rt") as fin:
                for line in fin:
                    _line = line.replace("\n", "").split("\t")
                    subj=_line[0]
                    pred=_line[1]
                    obj=_line[2]
                    if dictWikidata.get(subj) is not None and dictWikidata.get(obj) is not None:
                        fout.write(line)
                    #else:
                    #    if dictWikidata.get(obj) is not None:
                    #        fout.write(line)

def revertTriples():
    with gzip.open('wikidataTriplesFilteredRev.txt.gz', "wt") as fout:
        with gzip.open('wikidataTriplesFiltered.txt.gz', "rt") as fin:
            for line in fin:
                _line = line.replace("\n", "").split("\t")
                subj = _line[0]
                pred = _line[1]
                obj = _line[2]
                fout.write(obj+"\t"+pred+"\t"+subj+"\n")


def countSubjObjByPred(fileWikidataIdLinks):
    dictWikidata={}
    with open(fileWikidataIdLinks, "r") as fin:
        for line in fin:
            _line=line.replace("\n","").split("\t")
            dictWikidata[_line[1]]=1
    with gzip.open('wikidataSubjPredCount2.txt.gz',"wt") as foutSubj:
        with gzip.open('wikidataTriplesFiltered.txt.gz', "rt") as fin:
                aux=""
                dictSubjs={}
                for line in fin:
                    _line = line.replace("\n", "").split("\t")
                    subj=_line[0]
                    pred=_line[1]
                    obj=_line[2]
                    rel = dictWikidata.get(subj)
                    if rel is not None:
                        if subj!=aux :
                            for k, v in dictSubjs.items():
                                for _pred, _objs in v.items():
                                    foutSubj.write(k+"\t"+_pred+"\t"+str(_objs)+"\n")
                            dictSubjs={}
                            dictSubjs[subj]={pred:1}
                            aux=subj
                        else:
                            aux=subj
                            if dictSubjs.get(subj) is None:
                                dictSubjs[subj] = {pred: 1}
                            else:
                                if dictSubjs.get(subj).get(pred) is None:
                                    dictSubjs[subj][pred] = 1
                                else:
                                    dictSubjs[subj][pred]+=1

                for k, v in dictSubjs.items():
                    for _pred, _objs in v.items():
                        foutSubj.write(k + "\t" + _pred + "\t" + str(_objs) + "\n")


    with gzip.open('wikidataObjPredCount2.txt.gz', "wt") as foutObj:
        with gzip.open('triplesReverseSorted.txt.gz', 'rt') as fin:
            auxO=""
            dictObjs={}
            for line in fin:
                _line = line.replace("\n", "").split("\t")
                obj=_line[0]
                pred=_line[1]
                subj=_line[2]
                rel=dictWikidata.get(obj)
                if rel is not None:
                    if obj!=auxO:
                        for k, v in dictObjs.items():
                            for _pred, _subjs in v.items():
                                    foutObj.write(k+"\t"+_pred+"\t"+str(_subjs)+"\n")
                        dictObjs={}
                        dictObjs[obj]={pred:1}
                        auxO=obj
                    else:
                        auxO=obj
                        if dictObjs.get(obj) is None:
                            dictObjs[obj] = {pred: 1}
                        else:
                            if dictObjs.get(obj).get(pred) is None:
                                dictObjs[obj][pred] = 1
                            else:
                                dictObjs[obj][pred]+=1
            for k, v in dictObjs.items():
                for _pred, _subjs in v.items():
                    foutObj.write(k + "\t" + _pred + "\t" + str(_subjs) + "\n")

if __name__ == '__main__':
    args = sys.argv[1:]
    option=args[0]
    file = args[1]
    if option=='1':
        #filetriples, fileIDs
        filterTriples(args[1], args[2])
    else:
        if option=='2':
            revertTriples()
        else:
            if option == '3':
                countSubjObjByPred(file)
