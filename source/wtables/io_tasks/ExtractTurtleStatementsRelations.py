import gzip

prefEntity="<http://www.wikidata.org/entity/Q"
prefStatement="<http://www.wikidata.org/entity/statement/"
prefDirectProp="<http://www.wikidata.org/prop/direct/"
prefProp="<http://www.wikidata.org/prop/P"
prefStatementProp="<http://www.wikidata.org/prop/statement/"
def writeTriples(dictTriples, fout):
    for subj, statement in subjRelTemp.items():
        for pred, value in statement.items():
            fout.write(subj+"\t"+pred+"\t"+value)
def getId(uri):
    _uri=uri.replace(">","").replace("<","").split("/")
    return _uri[len(_uri)-1]

def extractTriples(fileIn, fileOut):
    subjRelTemp={}
    subjTemp=""
    isStatement=False
    with gzip.open(fileOut,"wt") as fout:
        with gzip.open(fileIn, "rt") as fin:
            for line in fin:
                _line=line.replace("\n","").split(" ")
                subj=_line[0]
                pred=_line[1]
                obj=_line[2]
                #If subject is an entity
                if prefEntity in subj:
                        #If subject is a new entity write older and clean
                        if pred.startswith(prefDirectProp) or pred.startswith(prefProp):
                            if obj.startswith(prefStatement):
                                subjRelTemp[obj]={pred:subj}
                            else:
                                if obj.startswith(prefEntity):
                                    fout.write(getId(subj) + "\t" + getId(pred) + "\t" + getId(obj)+"\n")

                #If subject is an statement
                else:
                    if subj.startswith(prefStatement):
                        predSubj = subjRelTemp.get(subj)
                        if obj.startswith(prefEntity):
                            if predSubj!=None:
                                k= list(predSubj.keys())[0]
                                if k==pred:
                                    fout.write(getId(predSubj.get(k)) + "\t" + getId(k)+"$" + "\t" + getId(obj)+"\n")
                                else:
                                    if pred.startswith(prefStatementProp):
                                        kname = k.replace(">", "").split("/")
                                        kname = kname[len(kname) - 1]
                                        predname = pred.replace(">", "").split("/")
                                        predname = predname[len(predname) - 1]
                                        if kname == predname:
                                            fout.write(getId(predSubj.get(k))+ "\t" + getId(k) + "$" + "\t" + getId(obj)+"\n")
                                            subjRelTemp={}

                        else:
                            if pred.startswith(prefStatementProp):
                                if predSubj != None:
                                    k = list(predSubj.keys())[0]
                                    kname=k.replace(">","").split("/")
                                    kname=kname[len(kname)-1]
                                    predname = pred.replace(">", "").split("/")
                                    predname = predname[len(predname) - 1]
                                    if kname==predname:
                                        fout.write(getId(predSubj.get(k)) + "\t" + getId(k)+"$" + "\t" + "-"+"\n")
                                        subjRelTemp={}


extractTriples("ntTest.nt.gz","outTriples.gz")