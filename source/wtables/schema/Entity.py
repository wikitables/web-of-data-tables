import json

class Entity (object):

    def __init__(self, entityId):
        self.entityId=entityId
        self.entityName=''
        self.entityURI=''
        self.parentClasses=[]
        self.numberChilds=0

    def setEntityName(self, entityName):
        self.entityName=entityName

    def setEntityURI(self, entityURI):
        self.entityURI=entityURI

    def setParentClasses(self, parentClasses):
        self.parentClasses=parentClasses

    def setNumberChilds(self, numberChilds):
        self.numberChilds=numberChilds

    def reprJSON(self):
        if len(self.parentClasses)>0:
            return dict(entityId=self.entityId, parentClasses=self.parentClasses)
        else:
            return dict(entityId=self.entityId, numberChilds=self.numberChilds)

class EntityComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'reprJSON'):
            return obj.reprJSON()
        else:
            return json.JSONEncoder.default(self, obj)

class EntityComplexDecoder(object):
    def default(self, obj):
        entity=None
        if 'entityId' in obj:
            entity=Entity(obj['entityId'])
            if obj.get('parentClasses')!=None:
                classes=obj['parentClasses']
                entityClasses=[]
                for c in classes:
                    ec=Entity(c['entityId'])
                    ec.setNumberChilds(c['numberChilds'])
                    entityClasses.append(ec)
                entity.setParentClasses(entityClasses)
        return entity

if __name__ == '__main__':
    a=Entity('Q1')
    c1=Entity('C1')
    c1.setNumberChilds(5)
    c2=Entity('C2')
    c2.setNumberChilds(6)
    classes=[c1,c2]
    a.setParentClasses(classes)

    fout=open('testEntityJson.json', 'w')
    fout.write(json.dumps(a.reprJSON(), cls=EntityComplexEncoder, skipkeys=True))
    fout.close()

    fin=open('testEntityJson.json','r')
    obj = fin.read()
    print(obj)
    obj = json.loads(obj)
    entity = EntityComplexDecoder().default(obj)
    print("entity:", entity.entityId)
    classes=entity.parentClasses
    print('classes')
    for c in classes:
        print(c.entityId, c.numberChilds)
