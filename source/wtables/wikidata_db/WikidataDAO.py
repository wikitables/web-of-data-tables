from wtables.utils.ParseLink import *
import numpy as np
class WikidataDAO(object):
    PRED_MAX_INSTA = 84004251
    PRED_MAX_SUBJ = 45783598
    PRED_MAX_OBJ = 45783598
    def __init__(self, fileParams):
        self.dictProperties={}
        self.dictRelations={}
        self.dictLinks={}
        self.dictPropStats = {}
        self.dictWikipediaRed = {}
        self.dictClassRange = {}
        self.dictClassDomain = {}
        self.dictSubjProp = {}
        self.dictObjProp = {}
        self.dictEntityClasses={}
        self.fileParams=fileParams

    def getWikidataID(self, link):
        wlink=wikiLink(link)

        wid= self.dictLinks.get(wlink)
        if wid==None:
            wlink=self.dictWikipediaRed.get(wlink)
            if wlink!=None:
                if "List_of" not in wlink:
                    wid=self.dictLinks.get(wlink)
                    return wid
        else:
            return wid

    def getWikidataProp(self,propID):
        return self.dictProperties.get(propID)

    def getRelationsURI(self, subjURI, objURI):
        wdSubj=self.dictLinks.get(wikiLink(subjURI))
        wdObj = self.dictLinks.get(wikiLink(objURI))
        relSubj=self.dictRelations.get(wdSubj)
        if relSubj!=None:
            relObj=relSubj.get(wdObj)
            if relObj!=None:
                return relObj
        return []

    def getRelations(self, wdSubj, wdObj):
        relSubj=self.dictRelations.get(wdSubj)
        if relSubj!=None:
            relObj=relSubj.get(wdObj)
            if relObj!=None:
                return relObj
        return []

    def getRelationsComplete(self, wdSubj, wdObj):
        relSubj = self.dictRelations.get(wdSubj)
        if relSubj != None:
            relObj = relSubj.get(wdObj)
            if relObj != None:
                preds=[]
                for pred in relObj:
                    preds.append(self.getWikidataProp(pred))
                return preds
        return []

    def getObjBySubjProp(self, wdSubj, wdtProp):
        subj=self.dictSubjProp.get(wdSubj)
        if subj!=None:
            obj=self.dictSubjProp.get(wdSubj).get(wdtProp)
            if obj==None:
                return 0
            return obj
        else:
            return 0

    def exist(self, wdSubj, wdtProp, wdObj):
        subj=self.dictSubjProp.get(wdSubj)
        if subj!=None:
            objs=self.dictSubjProp.get(wdSubj).get(wdtProp)
            if objs!=None:
                for obj in objs:
                    if obj==wdObj:
                        return 1
        return 0

    def getSubjByObjProp(self, wdObj, wdtProp):
        obj=self.dictObjProp.get(wdObj)
        if obj!=None:
            subj=obj.get(wdtProp)
            if subj==None:
                return 0
            else:
                return subj
        else:
            return 0

    def getDomain(self, pred):
        classes=self.dictClassDomain.get(pred)
        if classes is not None:
            return classes
        else:
            print("None Domain class: ", pred)
            return []

    def getRange(self, pred):
        classes = self.dictClassRange.get(pred)
        if classes is not None:
            return classes
        else:
            print("None Range class: ", pred)
            return []

    def getClasses(self, entity):
        return self.dictEntityClasses.get(entity)

    def isInDomain(self, wdSubj, pred):
        entityWD1_classes = self.getClasses(wdSubj)
        #print("classes Domain: ", pred, wdSubj,entityWD1_classes)
        if entityWD1_classes is None:
            return 2
        domain = self.getDomain(pred)
        intersectDomain = set(entityWD1_classes).intersection(domain)
        if len(intersectDomain)>0:
            return 1
        else:
            return 0


    def isInRange(self, wdObj, pred):
        entityWD2_classes = self.getClasses(wdObj)
        #print("classes range", pred, wdObj, entityWD2_classes)
        if entityWD2_classes is None:
            return 2
        range = self.getRange(pred)
        intersectRange = set(entityWD2_classes).intersection(range)
        if len(intersectRange)>0:
            return 1
        else:
            return 0

    def fillPropName(self):
        folder=self.fileParams.get('wikidata_files')
        filePropWikidata=os.path.join(folder,self.fileParams.get('wikidata_prop'))
        with open(filePropWikidata, "r") as fileProp:
            for line in fileProp:
                _line=line.replace("\n","").split("\t")
                self.dictProperties[_line[0]]=PropertyStat(_line[0], _line[1])
                self.dictProperties[_line[0]+"-1"] = PropertyStat(_line[0]+"-1", _line[1])

    def fillDomainRange(self):
        folder = self.fileParams.get('wikidata_files')
        fileClasses = os.path.join(folder, self.fileParams.get('wikidata_entity_classes'))
        fileDomain = os.path.join(folder, self.fileParams.get('wikidata_prop_domain'))
        fileRange = os.path.join(folder, self.fileParams.get('wikidata_prop_range'))
        cont=0
        with gzip.open(fileClasses, "rt") as fclasses:
            for line in fclasses:
                #print("Line class: ", cont)
                cont+=1
                _line = line.replace("\n", "").split("\t")
                entity = _line[0]
                classes = eval(_line[1])
                self.dictEntityClasses[entity] = classes
        print("Total entity classes: ", len(self.dictEntityClasses))
        oldProp=""
        line_count=0
        with gzip.open(fileDomain, "rt") as fileIn:
            for line in fileIn:
                #print("Line domain: ", line_count)
                line_count += 1
                split = line.replace("\n", "").split("\t")
                property = split[0]
                classp = eval(split[1])
                total = np.sum(np.array(list(classp.values())))
                #dictClasses={}
                #for k, v in classp.items():
                #    dictClasses[k]=float(v)/total
                self.dictClassDomain[property]=set(list(classp.keys())) #dictClasses


        oldProp=""
        line_count=0
        with gzip.open(fileRange, "rt") as fileIn:
            for line in fileIn:
                #print("Line range: ", line_count)
                line_count += 1
                split = line.replace("\n", "").split("\t")
                property = split[0]
                classp = eval(split[1])
                total = np.sum(np.array(list(classp.values())))
                #dictClasses = {}
                #for k, v in classp.items():
                    #dictClasses[k] = float(v) / total
                self.dictClassRange[property]=set(list(classp.keys()))#dictClasses


    def fillData(self):
        folder=self.fileParams.get('wikidata_files')
        filePropWikidata=os.path.join(folder,self.fileParams.get('wikidata_prop'))
        fileWikipediaRed=os.path.join(folder,self.fileParams.get('wikidata_redirects'))
        filePropStats=os.path.join(folder,self.fileParams.get('wikidata_prop_stats'))
        fileLinks=os.path.join(folder,self.fileParams.get('wikidata_links'))
        fileRelations=os.path.join(folder,self.fileParams.get('wikidata_rels'))

        with open(filePropWikidata, "r") as fileProp:
            for line in fileProp:
                _line=line.replace("\n","").split("\t")
                self.dictProperties[_line[0]]=PropertyStat(_line[0], _line[1])
                self.dictProperties[_line[0]+"-1"] = PropertyStat(_line[0]+"-1", _line[1])

        with open(fileWikipediaRed, "r") as fileProp:
            for line in fileProp:
                _line=line.replace("\n","").split("\t")
                self.dictWikipediaRed[_line[0]]=_line[1].replace("%20","_")

        with open(filePropStats, "r") as fileProp:
            for line in fileProp:
                _line=line.replace("\n","").split("\t")
                if self.dictProperties.get(_line[0])!=None:
                    self.dictProperties[_line[0]].setTimes(int(_line[1]))
                    self.dictProperties[_line[0]].setUniqSubj(int(_line[2]))
                    self.dictProperties[_line[0]].setUniqObj(int(_line[3]))

        with open(fileLinks, "r") as fileProp:
            for line in fileProp:
                _line=line.replace("\n","").split("\t")
                self.dictLinks[_line[0]]=_line[1]

        with open(fileRelations, "r") as fileProp:
            for line in fileProp:
                _line = line.replace("\n", "").split("\t")
                subj=_line[0]
                prop=_line[1]
                obj = _line[2]
                subjex=self.dictRelations.get(subj)
                if subjex!=None:
                    objex = self.dictRelations.get(subj).get(obj)
                    if objex!=None:
                        objex.add(prop)
                        self.dictRelations[subj][obj]=objex
                    else:
                        self.dictRelations[subj][obj]={prop}
                else:
                    self.dictRelations[subj] = {obj: {prop}}

                subjex = self.dictSubjProp.get(subj)
                if subjex != None:
                    objex = self.dictSubjProp.get(subj).get(prop)
                    if objex != None:
                        self.dictSubjProp[subj][prop]= objex+1
                    else:
                        self.dictSubjProp[subj][prop] = 1
                else:
                    self.dictSubjProp[subj] = {prop: 1}

                objex = self.dictObjProp.get(obj)
                if objex != None:
                    subjex = self.dictObjProp.get(objex).get(prop)
                    if subjex != None:
                        self.dictObjProp[objex][prop] = subjex+1
                    else:
                        self.dictObjProp[objex][prop] = 1
                else:
                    self.dictObjProp[objex] = {prop: 1}




class PropertyStat(object):

    def __init__(self, propId, propName):
        self.propId=propId
        self.propName = propName
        self.uniqSubj = 0
        self.uniqObj = 0
        self.times = 0


    def setUniqSubj(self, uniqSubj):
        self.uniqSubj=uniqSubj

    def setUniqObj(self, uniqObj):
        self.uniqObj=uniqObj

    def setTimes(self, times):
        self.times=times










