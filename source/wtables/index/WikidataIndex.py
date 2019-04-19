from whoosh.index import create_in
from whoosh.fields import *
from whoosh.index import open_dir
from whoosh.query import *
import os
from whoosh.qparser import QueryParser
import gzip

class WikidataIndex(object):

    def __init__(self, fileParams):
        #if os.path.exists(WIKIPEDIA_INDEX):
        #    self.wikipediaIndex = open_dir(WIKIPEDIA_INDEX)
        #if os.path.exists(REDIRECTS_INDEX):
        #    self.redirectsIndex = open_dir(REDIRECTS_INDEX)
        folder=fileParams.get('wikidata_files')
        dirWikidataIndex = fileParams.get('wikidata_relations_index')
        if dirWikidataIndex is None:
            raise Exception("Wikidata index param not found")
        else:
            path=os.path.join(folder, dirWikidataIndex)
            print(path, os.path.exists(path))
            if os.path.exists(path):
                self.relationsIndex = open_dir(path)
            else:
                raise Exception("Relation index not found")

    def createWikipediaIndex(fileIn):
        schema = Schema(url=TEXT(stored=True), wd=TEXT(stored=True))
        if not os.path.exists(WIKIPEDIA_INDEX):
            os.mkdir(WIKIPEDIA_INDEX)
        ix = create_in(WIKIPEDIA_INDEX, schema)
        writer = ix.writer()
        with gzip.open(fileIn, "rt") as filer:
            for line in filer:
                _line = line.replace("\n", "").split("\t")
                _url = _line[0]
                _wd = _line[1].split('/')[len(_line[1].split('/'))-1]
                writer.add_document(url=_url, wd=_wd)
        writer.commit()
        print("Wikipedia Index created successfully")

    def createRelationsIndex(fileIn):
        schema = Schema(subj=TEXT(stored=True), pred=TEXT(stored=True),
                        obj=TEXT(stored=True))
        if not os.path.exists(RELATIONS_INDEX):
            os.mkdir(RELATIONS_INDEX)
            ix = create_in(RELATIONS_INDEX, schema)
        else:
            ix= open_dir(RELATIONS_INDEX)
            print("Folder exist")
        writer = ix.writer(procs=8, limitmb=1024, multisegment=True)
        #writer = ix.writer()
        cont=0
        with gzip.open(fileIn, "rt") as filer:
            for line in filer:
                print("Line: ", cont)
                cont+=1
                _line = line.replace("\n", "").split("\t")
                _subj = _line[0]
                _pred = _line[1]
                _obj = _line[2]
                writer.add_document(subj=_subj, pred=_pred, obj=_obj)
        writer.commit()
        print("Relations Index created successfully")

    def createRedirectIndex(fileIn):
        schema = Schema(url=TEXT(stored=True), redirect=TEXT(stored=True))
        if not os.path.exists(REDIRECTS_INDEX):
            os.mkdir(REDIRECTS_INDEX)
        ix = create_in(REDIRECTS_INDEX, schema)
        writer = ix.writer()
        with gzip.open(fileIn, "rt") as filer:
            for line in filer:
                _line = line.replace("\n", "").split("\t")
                _url = _line[0]
                _redirect = _line[1]
                writer.add_document(url=_url, redirect=_redirect)
        writer.commit()
        print("Redirect Index created successfully")

    def getWikidataID(self, url):
        with self.wikipediaIndex.searcher() as searcher:
            query = QueryParser("url", self.wikipediaIndex.schema).parse(url)
            results = searcher.search(query)
            if len(results) > 0:
                return results[0]["wd"]
            else:
                return None

    def getRedirect(self, url):
        with self.redirectsIndex.searcher() as searcher:
            query = QueryParser("url", self.redirectsIndex.schema).parse(url)
            results = searcher.search(query)
            if len(results) > 0:
                return results[0]["redirect"]
            else:
                return None


    def getRelations(self, subj, obj):
        listRelations = []
        with self.relationsIndex.searcher() as searcher:
            query = QueryParser("pred", self.relationsIndex.schema).parse(u"AND (subj:"+subj+" obj:"+obj+")")

            results = searcher.search(query, limit=None)
            for res in results:
                listRelations.append(res['pred'])
        return listRelations

    def getObject(self, subj, pred):
        listRelations = []
        with self.relationsIndex.searcher() as searcher:
            query = QueryParser("obj", self.relationsIndex.schema).parse(u"AND (subj:"+subj+" pred:"+pred+")")

            results = searcher.search(query, limit=None)
            for res in results:
                listRelations.append(res['obj'])
        return listRelations

    def getSubject(self, obj, pred):
        listRelations = []
        with self.relationsIndex.searcher() as searcher:
            query = QueryParser("subj", self.relationsIndex.schema).parse(u"AND (obj:"+obj+" pred:"+pred+")")

            results = searcher.search(query, limit=None)
            for res in results:
                listRelations.append(res['subj'])
        return listRelations



    def addRedirectIndex(self, _url, _redirect):
        writer = self.redirectsIndex.writer()
        writer.add_document(url=_url, redirect=_redirect)
        writer.commit()


if __name__ == '__main__':
    WIKIPEDIA_INDEX = "/home/jluzuria/index/wikipediaIndex"
    RELATIONS_INDEX = "/home/jluzuria/index/relationsIndex"
    REDIRECTS_INDEX = "/home/jluzuria/index/redirectionIndex"
    RELATIONS_INDEX = "/home/jluzuria/index/relationsIndex"
    args = sys.argv[1:]
    if args[0]=="1":
        #flinks=args[1]
        frelations = args[1]
        #fredirects = args[3]
        #WikidataIndex.createWikipediaIndex(flinks)
        WikidataIndex.createRelationsIndex(frelations)
        #WikidataIndex.createRedirectIndex(fredirects)
        #params={'wikidata_relations_index':'index/relationsIndex','wikidata_files':'/home/jluzuria/WIKIDATA_DB1'}
        #wiki=WikidataIndex(params)
        #print(wiki.getSubject('Q386670','P54'))
        #print(wiki.getWikidataID('https://sv.wikipedia.org/wiki/Belgien'))
        #print(wiki.getRedirect('https://af.wikipedia.org/wiki/Bozen'))
