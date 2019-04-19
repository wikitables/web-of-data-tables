import gzip
from wtables.wikidata_db.WikidataDAO import WikidataDAO
from wtables.wikidata_db.ConfigProperties import ConfigProperties
import sys
import numpy as np
import traceback


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
        with gzip.open(fileOut, 'wt') as fout:
            with gzip.open(FILE_TRIPLES, "rt") as fin:
                for line in fin:
                    print('Line: ', cline)
                    cline+=1
                    _line = np.array(line.replace("\n", "").split("\t"))
                    features =list(_line[0:49])
                    featuresCluster = list(_line[55:65])
                    print("f1: ", features)
                    print("f2: ", featuresCluster)

                    f21=int(features[20].split(":")[1])
                    f22= int(features[21].split(":")[1])
                    f43= int(features[42].split(":")[1])
                    f44= int(features[43].split(":")[1])
                    if (f21==1 or f21==-1) and (f22==1 or f22==-1) and (f43==1 or f43==-1) and (f44==1 or f44==-1):
                        print(f21, f22, f43, f44)
                        triple = list(_line[65:len(_line)])
                        subj = triple[5].split(" :")
                        subj_id = subj[1]
                        pred = triple[6].split(" :")
                        pred_id = pred[0]
                        obj = triple[7].split(" :")
                        obj_id = obj[1]
                        print(subj_id, pred_id, obj_id, features[48])

                        features1 = [f.split(":")[1] for f in features]
                        features2 = [f.split(":")[1] for f in featuresCluster]

                        #for i, f in enumerate(features):
                        numObj = wikidataDAO.getObjBySubjProp(subj_id, pred_id)
                        numSubj = wikidataDAO.getSubjByObjProp(obj_id, pred_id)
                        exist = wikidataDAO.exist(subj_id, pred_id, obj_id)
                        print('f48:', features[48])
                        features1[48] = str(numObj)
                        features1[48] = str(numObj)

                        features1.append(str(numSubj))
                        print(featuresCluster[7], featuresCluster[5])
                        print(featuresCluster[8], featuresCluster[9])
                        #ratio108_106 = float(features2[7]) / float(features2[5])
                        #ratio110_109 = float(features2[8]) / float(features2[9])
                        #features2.append(ratio108_106)
                        #features2.append(ratio110_109)
                        #features1=np.array(features1)
                        #features2 = np.array(features2)
                        #features1=features1.astype(np.str)
                        #features2 = features2.astype(np.str)
                        triple.append(str(exist))
                        fout.write("\t".join(features1)+"\t"+"\t".join(features2)+"\t"+"#"+"\t"+"\t".join(triple)+"\n")

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
    #wikidataDAO.fillData()
    #wikidataDAO.fillSubjObjCount()
    FILE_TRIPLES = args[0]
    FILE_OUT =args[1]
    convertFeaturesToFrame(FILE_TRIPLES, FILE_OUT)
    #addFeatures2(FILE_TRIPLES, FILE_OUT)
    #FILE_SAMPLE = args[1]
    #replaceFeatures(FILE_TRIPLES, FILE_SAMPLE)


