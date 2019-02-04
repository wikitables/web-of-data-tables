
class FeaturesCluster(object):

    def __init__(self, cols):
        self.cols=cols
        self.subjects=[]
        self.objects=[]
        self.relations=[]
        self.relationsByProperty={}
        self.rowsByProperty = {}
        self.dictFeatures={}

    def addRowsByProperty(self, prop):
        if self.rowsByProperty.get(prop) is None:
            self.rowsByProperty[prop]=1
        else:
            self.rowsByProperty[prop] +=1

    def getProperties(self):
        return list(self.relationsByProperty.keys())

    def addRelationsByProperty(self, prop, relations):
        if self.relationsByProperty.get(prop) is None:
            self.relationsByProperty[prop]=relations
        else:
            self.relationsByProperty[prop].extend(relations)

    def addSubjects(self, subjects):
        self.subjects.extend(subjects)

    def addObjects(self, objects):
        self.objects.extend(objects)

    def addRelations(self, relations):
        self.relations.extend(relations)

    def calcFeatures(self):
        subj = len(self.subjects)
        usubj = len(set(self.subjects))
        obj = len(self.objects)
        uobj = len(set(self.objects))
        prelations = len(self.relations)
        uprelations = len(set(self.relations))
        dictRelByProp = {}
        for prop, relations in self.relationsByProperty.items():
            dictRelByProp[prop] = (len(relations), len(set(relations)))
            rowsProperty=self.rowsByProperty.get(prop,0)
            if rowsProperty==0:
                print("error1")
            dictp={101: subj, 102: obj, 103: usubj, 104: uobj, 105: prelations, \
                                          106: uprelations, 107: len(relations), 108: len(set(relations)),\
                                          109: rowsProperty}
            if self.dictFeatures.get(self.cols)==None:
                self.dictFeatures[self.cols] = {prop: dictp}
            else:
                self.dictFeatures[self.cols][prop] = dictp


