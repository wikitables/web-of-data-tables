# -*- coding: utf-8 -*-

from nltk.stem.snowball import SnowballStemmer
from nltk.stem.porter import PorterStemmer
import re
#import textdistance
from similarity.jarowinkler import JaroWinkler
from similarity.sorensen_dice import SorensenDice

class TextProcessing(object):

    def __init__(self):

        self.stemmer=SnowballStemmer("english")
        self.jaroWinkler = JaroWinkler()
        self.diceScore = SorensenDice()

    def removeStopWords(slef,text):
        stopList = {'the', 'of', 'by', 'in', 'on', 'at', 'for', 'an'}
        textsplited=text.replace("_"," ").split(" ")
        return " ".join([w for w in textsplited if w not in stopList])

    def removeSpecialCharacters2(self, text):
        #_text = text.replace("\'", "")
        pos = text.split("__")
        if len(pos) > 1:
            _text =pos[1]
            pos=pos[0]+"__"
        else:
            pos=''
            _text = text
        _text = _text.replace("_"," ")
        _text = _text.replace('&', ' and ').strip()
        _text = _text.replace('%', ' percentage ').strip()
        _text = _text.replace('#', ' no. ').strip()
        _text = _text.replace('$', ' currency ').strip()
        characters=['/','\\','>','<',"'s","(s)",'\"',"[","]","(",")","{","}","."]
        for c in characters:
            if c in _text:
                _text=_text.replace(c,' ').strip()
        _text = re.sub('\\s+', ' ', _text).strip()
        return pos+_text.replace(" ","_")
            #result = re.sub(r'[?|$|.|!\-\[\]/\(\)#,:]',r'', result)

    def stemWord(self, text):
        pos = text.split("__")
        if len(pos)>1:
            _text = pos[1]
            pos=pos[0]+"__"
        else:
            pos=''
            _text=text
        _text = " ".join(_text.split("_"))
        result= [self.stemmer.stem(t) for t in _text.split(" ")]
        result=" ".join(result)

        return pos+result.strip().replace(" ","_")


    def textSimilarity(self,text1, text2):
        score1=self.jaroWinkler.similarity(text1,text2)
        score2 = self.diceScore.similarity(text1,text2)
        return min([score1, score2])


    def cleanCellHeader(self, cellText):
        _cellText = re.sub('\\s+', ' ', cellText).strip()
        _cellText=self.removeSpecialCharacters2(_cellText)
        _cellText=self.stemWord(_cellText)
        return _cellText

    def orderHeaders(self, headers):
        _headers=headers[:]
        hd = {hi: [] for hi in _headers}
        for i, hi in enumerate(_headers):
            hd[hi].append(i)
        headersD = {}
        for k, v in hd.items():
            if len(v) > 1:
                _v = v
                _v.sort()
                i = 1
                for posh in _v:
                    headersD[posh] = str(i) + "__" + k
                    i += 1
            else:
                headersD[v[0]] = k
        _headers = []
        for i in range(len(headers)):
            _headers.append(headersD.get(i))
        return _headers

    def cleanTableHeader(self, headers):
        dataTypes = [h.split("@")[len(h.split("@")) - 1] for h in headers]
        _headers = [h.split("@")[0] for h in headers]
        _headers = [self.removeSpecialCharacters2(h) for h in _headers]
        _headers = [self.stemWord(h) for h in _headers]
        _headers = ['spancol' if hi == "" else hi for hi in _headers]
        hd = {hi: [] for hi in _headers}
        for i, hi in enumerate(_headers):
            hd[hi].append(i)
        headersD = {}
        for k, v in hd.items():
            if len(v) > 1:
                _v = v
                _v.sort()
                i = 1
                for posh in _v:
                    headersD[posh] = str(i) + "__" + k
                    i += 1
            else:
                headersD[v[0]] = k
        _headers = []
        for k, v in headersD.items():
            _headers.append(v + "@" + dataTypes[k])
        return _headers


"""if __name__ == '__main__':
    tp=TextProcessing()
    #print(tp.stemWord("runners_up"))
    #print(tp.cleanTableHeader(eval("['distance_(total;_km)@1', 'distance_(s2s;_km)@1', 'station_name_(transcribed)@3', 
                                        'station_name_(chosŏn'gŭl_(hanja))@3', 'former_name_(transcribed)@4', former_name_(chosŏn'gŭl_(hanja))@4, 'connections@3']")))
    fout=open("/home/jhomara/Desktop/web7/tableInformationNormalizedClean.out", "w")
    with open("/home/jhomara/Desktop/web7/tableInformationNormalized.out", "r") as filein:
        for line in filein:
            #print(line)
            _line=line.replace("\n","").split("\t")
            headers=eval(_line[1].replace('\"\"', ''))
            headers=tp.cleanTableHeader(headers)
            headers=[h for h in headers if 'spancol' not in h]
            if len(headers)>=2:
                fout.write(_line[0]+"\t"+str(headers)+"\t"+_line[3]+"\t"+_line[4]+"\n")
    fout.close()"""


