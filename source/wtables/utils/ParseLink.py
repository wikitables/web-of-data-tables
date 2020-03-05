# -*- coding: utf-8 -*-
from urllib.parse import quote
import urllib
#import requests
import sys
import os
from wtables.utils import Pipey
from wtables.utils.ResultCombiner import *
from wtables.utils.ParseLink import *

def wikiLink(link):
    if "cite_note" in link:
        return ""
    baseLink="https://en.wikipedia.org/wiki"
    link_ = link
    link_ = link_.replace("\"", "")
    link_ = link_.replace("''", "")
    if ("index.php" in link_):
        return ""
    if ("File:" in link_):
        return ""
    if "#" in link_:
        link_ = link_[0:link_.index("#")]
    if ("%23" in link_):
        link_ = link_[0:link_.index("%23")]
    if (link_.startswith("http")) :
        if ("wikipedia" in link_):
            return link_
        else:
            return ""
    else:
        if (link_.startswith(".")):
            link_ = link_.replace(".", "")
        if (link_.startswith("/")):
            link_ = link_.replace("//", "/")
    if link_ == "":
        return ""
    if "%" in link_:
        #print("Converted link: ", link_)
        return baseLink+link_
    else:
        link_ = link_.replace(" ", "_")
        link_ = link_.replace("%20", "_")
        if link_.startswith("/")==False:
            link_= quote(baseLink +"/"+ link_, safe='/:,()_')
        else:
            link_= quote(baseLink+link_, safe='/:,()_')
        if link_.endswith("/wiki"):
            return ""
        else:
            return link_


def getRedirectLink(link):
    _link=link.replace("_"," ")
    _link=urllib.parse.unquote(_link, encoding="utf-8")
    _link=_link.split("/")
    _link=_link[len(_link)-1]

    PARAMS = {
        'action': "query",
        'format': "json",
        'titles': _link,
        'redirects': "True",
        'formatversion': "2"
    }

    #headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    sessionR = None#requests.Session()
    response = sessionR.get(
        url='https://en.wikipedia.org/w/api.php?',
        params=PARAMS)
    rjson=response.json()
    try:
        if rjson!=None:
            query=rjson.get('query')
            if query!=None:
                redirects=query.get('redirects')
                if redirects!=None:
                    for red in redirects:
                        redirect1=red.get('to')
                        if redirect1!=None and redirect1!=_link:
                            return link +"\t" + wikiLink(redirect1)+'\n'
    except Exception:
        pass
    return link + "\t NOT FOUND" + "\n"

def readDocuments(input=0):
    # for each document that we want to process,
    cont=0
    with open(FILE_LINKS, "r") as fileLinks:
        for line in fileLinks:
            print("Line: ",cont)
            cont+=1
            yield line.replace("\n","")

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processDocuments(link):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    #print("File: ", file)
    result = getRedirectLink(link)
    yield result


if __name__ == '__main__':
    args = sys.argv[1:]
    FILE_LINKS = args[0]

    # newlink=getRedirectLink("https://de.wikipedia.org/wiki/Chang_Chung-jen")
    # print(newlink)
    # print(wikiLink("2013–14 UCI Track Cycling World Cup"))

    #ERROR_FILE = args[2]
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 12)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner(args[1]))
    pipeline.run(100)

