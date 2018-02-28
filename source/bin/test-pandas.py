import wikimarkup
import wikitextparser as wtp

import wikipedia as wp

import pandas as pd
from pyquery import PyQuery

def get_tables_1(wiki):
    html = PyQuery(wikimarkup.parse(wiki))
    frames = []
    for table in html('table'):
        data = [[x.text.strip() for x in row]
                for row in table.getchildren()]
        df = pd.DataFrame(data[1:], columns=data[0])
        frames.append(df)
    return frames


def get_tables_2(wiki):
    parsed = wtp.parse(wiki)
#    print(parsed.tables[0].data())
    frames = []
    for table in parsed.tables:
        print(table.data())
        print(table.caption)
        data = table.data()
        df = pd.DataFrame(data[1:], columns=data[0])
        print(df)
#    html = PyQuery(parsed)
#    frames = []
#    for table in html('table'):
#        data = [[x.text.strip() for x in row]
#                for row in table.getchildren()]
#        df = pd.DataFrame(data[1:], columns=data[0])
#        frames.append(df)
    return frames
    
    
def get_tables_3():
#    title = 'List of U.S. states by Hispanic and Latino population'
    title = 'FC_Barcelona'
    html = wp.page(title)  #.html().encode("UTF-8")
    print(html)
#    frames = []
#    df = pd.read_html(html)
#    print(len(df))
#    print(df)


def get_tables_4():
#    page = 'https://en.wikipedia.org/wiki/University_of_California,_Berkeley'
    page = 'https://en.wikipedia.org/wiki/FC_Barcelona'
    infoboxes = pd.read_html(page, index_col=0, attrs={"class":"infobox"})
    wikitables = pd.read_html(page, index_col=0, attrs={"class":"wikitable"})
    all_tables = pd.read_html(page, index_col=0)

    print("Extracted {num} infoboxes".format(num=len(infoboxes)))
    print("Extracted {num} wikitables".format(num=len(wikitables)))
    print("Extracted {num} all tables".format(num=len(all_tables)))
    
    print(all_tables[9])


def main():
    wiki = """
    =Title=

    Description.

    {| class="wikitable sortable"
    |-
    ! Model !! Mhash/s !! Mhash/J !! Watts !! Clock !! SP !! Comment
    |-
    | ION || 1.8 || 0.067 || 27 ||  || 16 || poclbm;  power consumption incl. CPU
    |-
    | 8200 mGPU || 1.2 || || || 1200 || 16 || 128 MB shared memory, "poclbm -w 128 -f 0"
    |-
    | 8400 GS || 2.3 || || || || || "poclbm -w 128"
    |-
    |}

    {| class="wikitable sortable"
    |-
    ! A !! B !! C
    |-
    | 0
    | 1
    | 2
    |-
    | 3
    | 4
    | 5
    |}
    """
#    get_tables_2(wiki)
    
    wiki_template = """
    {{Fs start}}
    {{Fs player|no= 1|pos=GK|nat=GER|name=[[Marc-André ter Stegen]]}}
    {{Fs player|no= 2|pos=DF|nat=POR|name=[[Nélson Semedo]]}}
    {{Fs player|no= 3|pos=DF|nat=ESP|name=[[Gerard Piqué]]}}
    {{Fs player|no= 4|pos=MF|nat=CRO|name=[[Ivan Rakitić]]}}
    {{Fs player|no= 5|pos=MF|nat=ESP|name=[[Sergio Busquets]]|other=[[Captain (association football)|3rd captain]]}}
    {{Fs player|no= 6|pos=MF|nat=ESP|name=[[Denis Suárez]]}}
    {{Fs player|no= 7|pos=MF|nat=TUR|name=[[Arda Turan]]}}
    {{Fs player|no= 8|pos=MF|nat=ESP|name=[[Andrés Iniesta]]|other=[[Captain (association football)|captain]]}}
    {{Fs player|no= 9|pos=FW|nat=URU|name=[[Luis Suárez]]}}
    {{Fs player|no=10|pos=FW|nat=ARG|name=[[Lionel Messi]]|other=[[Captain (association football)|vice-captain]]}}
    {{Fs player|no=11|pos=FW|nat=FRA|name=[[Ousmane Dembélé]]}}
    {{Fs player|no=12|pos=MF|nat=BRA|name=[[Rafinha (footballer, born February 1993)|Rafinha]]}}
    {{Fs player|no=13|pos=GK|nat=NED|name=[[Jasper Cillessen]]}}
    {{Fs mid}}
    {{Fs player|no=14|pos=DF|nat=ARG|name=[[Javier Mascherano]]|other=[[Captain (association football)|4th captain]]}}
    {{Fs player|no=15|pos=MF|nat=BRA|name=[[Paulinho (footballer)|Paulinho]]}}
    {{Fs player|no=16|pos=FW|nat=ESP|name=[[Gerard Deulofeu]]}}
    {{Fs player|no=17|pos=FW|nat=ESP|name=[[Paco Alcácer]]}}
    {{Fs player|no=18|pos=DF|nat=ESP|name=[[Jordi Alba]]}}
    {{Fs player|no=19|pos=DF|nat=FRA|name=[[Lucas Digne]]}}
    {{Fs player|no=20|pos=MF|nat=ESP|name=[[Sergi Roberto]]}}
    {{Fs player|no=21|pos=MF|nat=POR|name=[[André Gomes]]}}
    {{Fs player|no=22|pos=DF|nat=ESP|name=[[Aleix Vidal]]}}
    {{Fs player|no=23|pos=DF|nat=FRA|name=[[Samuel Umtiti]]}}
    {{Fs player|no=25|pos=DF|nat=BEL|name=[[Thomas Vermaelen]]}}
    {{Fs player|no=—|pos=MF|nat=BRA|name=[[Philippe Coutinho]]}}
    {{Fs end}}
    """
#    get_tables_2(wiki_template)
    get_tables_4()

if __name__ == "__main__":
    main()
