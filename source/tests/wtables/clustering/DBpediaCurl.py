import json
import pycurl
import io
import xml.etree.ElementTree as ET

def curlDBPedia(text):
    text=text+"."
    DB_url="http://api.dbpedia-spotlight.org/en/candidates"
    text = text.replace(" ", "%20")
    data = json.dumps({
        "H":"content-type:application/x-www-form-urlencoded",
        "d":"disambiguator=Document&confidence=0.2&support=20&text="+text
         })
    buffer =io.BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, DB_url)
    c.setopt(c.POSTFIELDS, data)
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()
    body = buffer.getvalue()
    strb= body.decode("utf-8")
    return getTypes(strb)

def getTypes(xml_text):
    """
    Get types of resources extracted from xml.
    :param xml_text:
    :return: List of types.
    """
    try:
        tree = ET.fromstring(xml_text)
    except ET.ParseError:
        print("Parse Error")
        return None
    d = {}
    for child in tree:
        for c in child:
            type=c.attrib["types"]
            if type!=None and type!="":
                type = type.split(", ")
                for i in type:
                    s = i.split(":")
                    if d.get(s[0]) == None:
                        d[s[0]] = [s[1]]
                    else:
                        d[s[0]].append(s[1])
    return d.get("DBpedia")

def addArticleEntitiesType(fileName):
    fo=open("cluster0_enriched.csv", "w")
    with open (fileName, "r", encoding="utf-8") as fi:
        for line in fi:
            l= line.replace("\n", "")
            text=l.split("\t")[6]
            ltypes=curlDBPedia(text)
            if(ltypes!=None and len(ltypes)>0):
                ltypes=set(ltypes)
                ltypes=list(ltypes)
                ltypes.sort()
            fo.write(l+"\t"+str(ltypes)+"\n")

    fo.close()

addArticleEntitiesType("cluster_parti_vote.csv")

