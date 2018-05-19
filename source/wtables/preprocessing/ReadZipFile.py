import bz2
from bs4 import BeautifulSoup
from source.wtables.preprocessing import ReadHTML as readHTML
from source.wtables.preprocessing.JsonArticle import *
import json

def readFile(path):
    bz_file = bz2.BZ2File(path)
    soup=BeautifulSoup(bz_file.read(), 'html.parser')
    title=readHTML.readTitle(soup)
    tables = readHTML.readTables(soup)
    tables2d=[]
    for t in tables:
        html,t2d=readHTML.tableTo2d(t)
        tables2d.append(t2d)
    article = Article(title,tables2d)
    writeArticle(article)

def writeArticle(article):
    f=open(article.title+".txt","w")
    print(article.reprJSON())
    f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
    f.close()

