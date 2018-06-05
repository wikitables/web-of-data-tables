import bz2
from bs4 import BeautifulSoup
from wtables.preprocessing import ReadHTML as readHTML
from wtables.preprocessing.JsonArticle import *
import json


def readFile(path):
    bz_file = bz2.BZ2File(path)
    soup=BeautifulSoup(bz_file.read(), 'html.parser')
    title=readHTML.readTitle(soup)
    tables = readHTML.readTables(soup)
    tables2d=[]
    for i,t in enumerate(tables):
        html,t2d=readHTML.tableTo2d(t,i)
        tables2d.append(t2d)
    article = Article(1,title,tables2d)
    writeArticle(article)


def writeArticle(article):
    f=open(article.title+".txt","w")
    f.write(json.dumps(article.reprJSON(), cls=ComplexEncoder, skipkeys=True))
    f.close()

