class TableCell(object):

    def __init__(self, tableId, row, col, colName):
        self.tableId=tableId
        self.row=row
        self.col=col
        self.colName=colName
        self.resources=[]
        self.formatFeatures={}

    def setResources(self, resources):
        self.resources=resources

    def setFormatFeatures(self, formatFeatures):
        self.formatFeatures=formatFeatures

    def getResourcesIds(self):
        resIds=[]
        for res in self.resources:
            resIds.append(res.id)
        return resIds


