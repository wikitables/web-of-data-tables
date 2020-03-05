
class TripleFeatures(object):

    def __init__(self, tableId, pos, subj, obj, predicates):
        self.tableId=tableId
        self.pos=pos
        self.subj=subj
        self.obj=obj
        self.features={}
        self.predicates=predicates
        self.colName1=None
        self.colName2=None
        self.cols=None

    def setFeatures(self, features):
        self.features=features.copy()

    def setCols(self, cols):
        self.cols=cols

    def setColName1(self, colName1):
        self.colName1=colName1

    def setColName2(self, colName2):
        self.colName2=colName2
