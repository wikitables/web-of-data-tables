import gzip
from wtables.wikidata_db.WikidataDAO import WikidataDAO
from wtables.wikidata_db.ConfigProperties import ConfigProperties
import sys
import numpy as np
import traceback
from wtables.preprocessing.TextProcessing import TextProcessing


def getPredicateFeatures(predId):
    #Extract features from stats of predicate

    # (17) Normalized unique subject count
    #if "-1" in pred.propId:
    _pred=wikidataDAO.getWikidataProp(predId)
    #else:
    dictFeatures={}

    dictFeatures[17] = _pred.uniqSubj / wikidataDAO.PRED_MAX_SUBJ
    dictFeatures[18] = _pred.uniqObj / wikidataDAO.PRED_MAX_OBJ
    dictFeatures[47] = _pred.maxSubj
    dictFeatures[48] = _pred.maxObj

    dictFeatures[19] = _pred.times / wikidataDAO.PRED_MAX_INSTA
    if dictFeatures[19]>0:
        dictFeatures[20] = dictFeatures[18]/dictFeatures[19]
    else:
        dictFeatures[20]=0
        print("Feature 19 0: ",_pred.propName)
    return dictFeatures

def convertFeaturesToFrame(fileTriples, fileOut):
    cline = 0
    with gzip.open(fileOut, 'wt') as fout:
        fout.write("\t".join([str(i) for i in np.arange(1,63)])+"\n")
        with gzip.open(FILE_TRIPLES, "rt") as fin:
            for line in fin:
                print('Line: ', cline)
                cline += 1
                _line = np.array(line.replace("\n", "").split("\t"))
                features = list(_line[0:62])
                triple = _line[62:len(_line)]
                features1=[f.split(":")[1].replace(".",",") for f in features]
                fout.write("\t".join(features1)+"\t"+'#'+"\t"+
                           "\t".join(triple)+"\n")

def addFeatures63(fileTriples, fileOut):
    cline = 0
    with gzip.open(fileOut, 'wt') as fout:
        with gzip.open(FILE_TRIPLES, "rt") as fin:
            for line in fin:
                print('Line: ', cline)
                cline += 1
                _line = np.array(line.replace("\n", "").split("\t"))
                features = list(_line[0:62])
                f63=float(features[56])/float(features[54])
                triple = _line[62:len(_line)]
                #features1=[f.split(":")[1].replace(".",",") for f in features]
                fout.write("\t".join(features)+"\t"+str(f63)+"\t"+
                           "\t".join(triple)+"\n")

def addFeatures28_29_30(fileTriples, fileOut):
    cline = 0
    with open(fileOut, 'w') as fout:
        with open(FILE_TRIPLES, "r") as fin:
            for line in fin:
                print('Line: ', cline)

                if cline==0:
                    fout.write(line)
                    cline += 1
                    continue
                cline += 1
                _line = np.array(line.replace("\n", "").split("\t"))
                #features = list(_line[0:6])
                #triple = _line[63:len(_line)]
                #print(triple[4],triple[5], triple[7])
                f28=textProcessing.textSimilarity(_line[64], _line[67])
                f29 = textProcessing.textSimilarity(_line[65], _line[67])
                f30 = max([f28,f29])
                _line[23]=str(f28).replace(".",",")
                _line[24]=str(f29).replace(".",",")
                _line[25]=str(f30).replace(".",",")
                #print(features[27],features[28],features[29])
                #features1=[f.split(":")[1].replace(".",",") for f in features]
                fout.write("\t".join(_line)+"\n")


def addFeatures2(FILE_TRIPLES, fileOut):
        cline = 0
        with gzip.open(fileOut, 'wt') as fout:
            with open(FILE_TRIPLES, "r") as fin:
                for line in fin:
                    print('Line: ', cline)

                    _line = np.array(line.replace("\n", "").split("\t"))
                    features = list(_line[0:60])
                    triple = list(_line[60:])
                    if cline==0:
                        print(features[44], features[45])
                        _line[44]='49'
                        _line[45] = '50'
                        fout.write("\t".join(_line) + "\n")
                        cline+=1
                        continue


                    #subj = triple[5].split(" :")
                    subj_id = triple[16].strip()
                    #pred = triple[6].split(" :")
                    pred_id = triple[17].strip()
                    #obj = triple[7].split(" :")
                    obj_id = triple[21].strip()
                    print(subj_id, pred_id, obj_id)
                    numObj = wikidataDAO.getObjBySubjProp(subj_id, pred_id)
                    numSubj = wikidataDAO.getSubjByObjProp(obj_id, pred_id)


                    print("nnum: ", numObj, numSubj)
                    features[44]=str(numObj)
                    features[45]=str(numSubj)
                    fout.write("\t".join(features) + "\t" + "\t".join(
                            triple) + "\n")
                    cline += 1

def addFeatures(FILE_TRIPLES, fileOut):
        cline=0
        features={}
        with gzip.open(fileOut, 'wt') as fout:
            with gzip.open(FILE_TRIPLES, "rt") as fin:
                for line in fin:
                    print('Line: ', cline)
                    cline+=1
                    _line = np.array(line.replace("\n", "").split("\t"))
                    f21=int(_line[20].split(":")[1])
                    f22= int(_line[21].split(":")[1])
                    f43= int(_line[42].split(":")[1])
                    f44= int(_line[43].split(":")[1])
                    if (f21==1 or f21==-1) and (f22==1 or f22==-1) and (f43==1 or f43==-1) and (f44==1 or f44==-1):
                        subj = _line[71].split(" :")
                        subj_id = subj[1]
                        pred = _line[72].split(" :")
                        pred_id = pred[0]
                        obj = _line[73].split(" :")
                        obj_id = obj[1]

                        domain = str(wikidataDAO.isInDomain(subj_id, pred_id))
                        rangeC = str(wikidataDAO.isInRange(obj_id, pred_id))

                        numObj = wikidataDAO.getObjBySubjProp(subj_id, pred_id)
                        numSubj = wikidataDAO.getSubjByObjProp(obj_id, pred_id)


                        features[39] = str(domain)
                        features[40] = str(rangeC)
                        objectsBySubj = wikidataDAO.getObjBySubjProp(subj_id, pred_id)
                        features[49] = str(objectsBySubj)
                        subjByObj = wikidataDAO.getSubjByObjProp(obj_id, pred_id)
                        features[50] = str(subjByObj)
                        print("Line: ", _line[38],_line[39],_line[48],_line[49])
                        _line[38] = '39:'+features.get(39)
                        _line[39] = '40:' + features.get(40)
                        _line[48] = '49:' + features.get(49)
                        _line[49] = '50:' + features.get(50)

                        fout.write("\t".join(_line)+"\n")


def addFeaturesNoLabel(FILE_TRIPLES, fileOut):
    cline = 0
    features = {}
    with open(fileOut, 'w') as fout:
        with open(FILE_TRIPLES, "r") as fin:
            for line in fin:
                print('Line: ', cline)
                if cline==0:
                    cline+=1
                    continue
                cline += 1
                _line = np.array(line.replace("\n", "").split("\t"))
                #f21 = int(_line[20].split(":")[1])
                #f22 = int(_line[21].split(":")[1])
                #f43 = int(_line[42].split(":")[1])
                #f44 = int(_line[43].split(":")[1])
                #if (f21 == 1 or f21 == -1) and (f22 == 1 or f22 == -1) and (f43 == 1 or f43 == -1) and (
                #        f44 == 1 or f44 == -1):

                print(_line[69], _line[70],_line[71])
                subj = _line[69].split(" :")
                subj_id = subj[1]
                pred = _line[70].split(" :")
                pred_id = pred[0]
                obj = _line[71].split(" :")
                obj_id = obj[1]

                domain = str(wikidataDAO.isInDomain(subj_id, pred_id))
                rangeC = str(wikidataDAO.isInRange(obj_id, pred_id))
                fout.write("\t".join(_line) +"\t"+str(domain)+"\t"+str(rangeC)+"\n")

def replaceFeatures(fileAllTriples, fileSampleTriples):
    listTriples=[]
    dictFeatures={}
    cont=0
    with gzip.open(fileSampleTriples, 'rt') as fin:
        for line in fin:
            _line=line.replace('\n','').split("\t")
            features = "\t".join(_line[0:65])
            triple = "\t".join(_line[65:len(_line)])
            dictFeatures[triple] = 1
    print("Triples sample: ", len(listTriples))
    newFeatures={}
    cont=0
    with open('newFeatures2.csv', 'w') as fout:
        with gzip.open(fileAllTriples, 'rt') as fin:
            for line in fin:
                print("Line: ", str(cont), len(dictFeatures))
                cont+=1
                _line=line.replace('\n','').split("\t")
                features = "\t".join(_line[0:65])
                triple = _line[65:len(_line)]
                striple="\t".join(triple)
                index=None
                oldFeatures=dictFeatures.get(striple)
                if oldFeatures is not None:
                    fout.write(features+"\t"+striple+"\n")
                    del dictFeatures[striple]
                if len(dictFeatures) == 0:
                    break;

    #print("Features: ",len(dictFeatures))
    #for k, v in dictFeatures.items():a
    #    print(listTriples[k])

if __name__ == '__main__':
    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()

    wikidataDAO= WikidataDAO(params)
    wikidataDAO.fillData()
    wikidataDAO.fillDomainRange()
    wikidataDAO.fillSubjObjCount()
    FILE_TRIPLES = args[0]
    FILE_OUT =args[1]
    addFeaturesUIU(FILE_TRIPLES, FILE_OUT)



