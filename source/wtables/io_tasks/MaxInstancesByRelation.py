import gzip
import sys

def maxObjectsBySubj(triplesFile):
    oldsubj = ""
    dictObj = {}
    dictNumObj = {}
    cont=0
    with gzip.open("triplesReverse.txt.gz", "wt") as fout:
        with gzip.open(triplesFile, "rt") as fin:
            for line in fin:
                print("Line: ", cont)
                cont+=1
                _line = line.replace("\n", "").split("\t")
                subj = _line[0]
                pred = _line[1]
                obj = _line[2]
                fout.write(obj+"\t"+pred+"\t"+subj+"\n")
                if oldsubj == "":
                    oldsubj = _line[0]
                    dictObj[subj] = {pred: 1}
                    continue
                if oldsubj == subj:
                    if dictObj.get(subj).get(pred) == None:
                        dictObj[subj][pred] = 1
                    else:
                        dictObj[subj][pred] += 1
                else:
                    for _subj, _rel in dictObj.items():
                        #print(_rel)
                        for _pred, nobj in _rel.items():
                            if dictNumObj.get(_pred) == None:
                                dictNumObj[_pred] = [nobj]
                            else:
                                dictNumObj[_pred].append(nobj)
                    dictObj={}
                    dictObj[subj] = {pred: 1}
                    oldsubj = subj
    fout=gzip.open("maxObjectsByPred.txt.gz","wt")
    for k, v in dictNumObj.items():
        fout.write(k+"\t"+str(max(v))+"\n")
    fout.close()

def maxSubjectsByObj(reverseTriplesFile):
    oldsubj = ""
    dictObj = {}
    dictNumObj = {}
    cont=0
    with gzip.open(reverseTriplesFile, "rt") as fin:
        for line in fin:
            print("Line: ", cont)
            cont+=1
            _line = line.replace("\n", "").split("\t")
            subj = _line[0]
            pred = _line[1]
            obj = _line[2]
            if oldsubj == "":
                oldsubj = _line[0]
                dictObj[subj] = {pred: 1}
                continue
            if oldsubj == subj:
                if dictObj.get(subj).get(pred) == None:
                    dictObj[subj][pred] = 1
                else:
                    dictObj[subj][pred] += 1
            else:
                for _subj, _rel in dictObj.items():
                    for _pred, nobj in _rel.items():
                        if dictNumObj.get(_pred) == None:
                            dictNumObj[_pred] = [nobj]
                        else:
                            dictNumObj[_pred].append(nobj)
                dictObj = {}
                dictObj[subj] = {pred:1}
                oldsubj = subj
    fout=gzip.open("maxSubjByPred.txt.gz","wt")
    for k, v in dictNumObj.items():
        fout.write(k+"\t"+str(max(v))+"\n")
    fout.close()

if __name__ == '__main__':
    args = sys.argv[1:]
    if args[0]=="1":
        maxObjectsBySubj(args[1])
    else:
        if args[0] == "2":
            maxSubjectsByObj(args[1])