from SPARQLWrapper import SPARQLWrapper, JSON
import sys
class WikidataSparql(object):
    def __init__(self):
        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        self.sparql.setReturnFormat(JSON)

    def getObject(self, subj, prop):
        self.sparql.setQuery("""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wd: <http://www.wikidata.org/entity/>
        SELECT distinct ?obj WHERE {
            wd:"""+subj+" wdt:"+prop+" ?obj.}")
        queryResults = self.sparql.query().convert()
        print(queryResults)
        for result in queryResults["results"]["bindings"]:
            return result["obj"]['value']

    def getPropertyName(self, prop):
        self.sparql.setQuery("""PREFIX wikibase: <http://wikiba.se/ontology#>
                PREFIX bd: <http://www.bigdata.com/rdf#>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?propLabel WHERE {
                ?prop wikibase:directClaim wdt:"""+prop+""" .     
                SERVICE wikibase:label 
                { bd:serviceParam wikibase:language "en". }
                }""")
        queryResults = self.sparql.query().convert()
        #print(queryResults)
        for result in queryResults["results"]["bindings"]:
            return result["propLabel"]['value']

if __name__ == '__main__':
    wikidataSparql=WikidataSparql()
    args = sys.argv[1:]
    file=args[0]
    fout=open(args[1],"w")
    cont=0
    with open(file,"r") as fin:
        for line in fin:
            print("Line: ", cont)
            cont+=1
            _line=line.replace("\n","")
            prop=_line.split("-1")[0]
            name=wikidataSparql.getPropertyName(prop)
            if name is None:
                print("Error: ", prop)
            else:
                fout.write(_line+"\t"+name+"@en"+"\n")
    fout.close()

    #print(wikidataSparql.getObject("Q6071828", "P136"))
