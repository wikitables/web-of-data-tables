import numpy as np
import pandas as pd
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

import matplotlib.pyplot as plt

flines=open('clusters_dbscan_text2.csv','r').readlines()
cont=0
for line in flines:
    if cont==0:
        cont+=1
        continue
    cluster=line.split('\t')[0]
    text = line.split('\t')[1]
    #fin=fin.replace("@","_")
    #print(fin[0])
    print(len(text.split()))
    wordcloud = WordCloud(max_font_size=50, max_words=100,
                          collocations=False,
                          background_color="white").generate(text)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig('db01_'+str(cluster)+".png")