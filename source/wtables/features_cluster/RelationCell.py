
class RelationCell(object):

    def __init__(self, tableCell1, tableCell2):
        self.tableCell1=tableCell1
        self.tableCell2=tableCell2
        self.predicates={}

    def setPredicates(self,predicates):
        self.predicates=predicates

    def toString(self):
        #print(self.predicates)
        res1=self.tableCell1.resources
        res2=self.tableCell2.resources
        for r1 in res1:
            obj = self.predicates.get(r1.id)
            if obj!=None:
                for r2 in res2:
                    pred=obj.get(r2.id)
                    if pred!=None:
                        return str(self.tableCell1.colName)+"\t"+str(self.tableCell2.colName)+"\t"+\
                            r1.id+"\t"+r2.id+"\t"+str(pred)+"\n"
