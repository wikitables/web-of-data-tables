# -*- coding: utf-8 -*-

import pytest
from bs4 import BeautifulSoup

from wtables.feature_extraction import clean_string
from wtables.feature_extraction import features_clustering
from wtables.feature_extraction import html_features, lexical_features, bow_header_features


def test_clean_string():
    str = 'Year'
    assert clean_string(str) == 'year'
    str = ' New Number 123'
    assert clean_string(str) == 'new_number'
    str = 'A & B: Letters'
    assert clean_string(str) == 'a_and_b:_letters'
    str = ' String  with     whitespaces:'
    assert clean_string(str) == 'string_with_whitespaces'


@pytest.mark.skip
def test_lexical_features():
    html = """<html>
    <head></head>
    <body>
        <TABLE class="wikitable">
          <TBODY>
           <TR>
            <TH>Year</TH>
            <TH>Champion</TH>
            <TH>Motorcycle</TH>
           </TR>
           <TR>
            <TD><A href="http://en.wikipedia.org/wiki/Supersport_World_Championship" title="Supersport World Championship">1997</A></TD>
            <TD><SPAN class="flagicon"><A href="http://en.wikipedia.org/wiki/Italy" title="Italy"><IMG alt="Italy" src="http://upload.wikimedia.org/wikipedia/en/thumb/0/03/Flag_of_Italy.svg/23px-Flag_of_Italy.svg.png" width="23" height="15" class="thumbborder"/></A></SPAN> <A href="http://en.wikipedia.org/wiki/Paolo_Casoli" title="Paolo Casoli">Paolo Casoli</A></TD>
            <TD>&nbsp;</TD>
           </TR>
          </TBODY>
         </TABLE>
    </body>
    </html>
    """
    soup = BeautifulSoup(html, 'html.parser')  # html.parser or lxml
    table_0 = soup.select('table')[0]
    fv = lexical_features(table_0)
    pass


@pytest.mark.skip
def test_html_features():
    html = """<html>
        <head></head>
        <body>
            <TABLE class="wikitable">
              <TBODY>
               <TR>
                <TH>Year</TH>
                <TH>Champion</TH>
                <TH>Motorcycle</TH>
               </TR>
               <TR>
                <TD><A href="http://en.wikipedia.org/wiki/Supersport_World_Championship" title="Supersport World Championship">1997</A></TD>
                <TD><SPAN class="flagicon"><A href="http://en.wikipedia.org/wiki/Italy" title="Italy"><IMG alt="Italy" src="http://upload.wikimedia.org/wikipedia/en/thumb/0/03/Flag_of_Italy.svg/23px-Flag_of_Italy.svg.png" width="23" height="15" class="thumbborder"/></A></SPAN> <A href="http://en.wikipedia.org/wiki/Paolo_Casoli" title="Paolo Casoli">Paolo Casoli</A></TD>
                <TD>&nbsp;</TD>
               </TR>
              </TBODY>
             </TABLE>
        </body>
        </html>
        """
    soup = BeautifulSoup(html, 'html.parser')  # html.parser or lxml
    table_0 = soup.select('table')[0]
    fv = html_features(table_0)


def test_bow_header_features():
    html = """
    <html>
    <head></head>
    <body>
        <TABLE class="wikitable">
          <TBODY>
           <TR>
            <TH>Year 1</TH>
            <TH>Year 2</TH>
            <TH>Total</TH>
           </TR>
           <TR>
            <TD><A href="http://en.wikipedia.org/wiki/Supersport_World_Championship" title="Supersport World Championship">1997</A></TD>
            <TD><SPAN class="flagicon"><A href="http://en.wikipedia.org/wiki/Italy" title="Italy"><IMG alt="Italy" src="http://upload.wikimedia.org/wikipedia/en/thumb/0/03/Flag_of_Italy.svg/23px-Flag_of_Italy.svg.png" width="23" height="15" class="thumbborder"/></A></SPAN> <A href="http://en.wikipedia.org/wiki/Paolo_Casoli" title="Paolo Casoli">Paolo Casoli</A></TD>
            <TD>&nbsp;</TD>
           </TR>
          </TBODY>
         </TABLE>
    </body>
    </html>
    """
    soup = BeautifulSoup(html, 'html.parser')  # html.parser or lxml
    table_0 = soup.select('table')[0]
    fv = bow_header_features(table_0)
    assert fv == {'TOKEN_total': 1.0, 'TOKEN_year': 2.0}


@pytest.mark.skip
def test_features_clustering():
    html = """<html>
        <head></head>
        <body>
            <TABLE class="wikitable">
              <TBODY>
               <TR>
                <TH>Year</TH>
                <TH>Champion</TH>
                <TH>Motorcycle</TH>
               </TR>
               <TR>
                <TD><A href="http://en.wikipedia.org/wiki/Supersport_World_Championship" title="Supersport World Championship">1997</A></TD>
                <TD><SPAN class="flagicon"><A href="http://en.wikipedia.org/wiki/Italy" title="Italy"><IMG alt="Italy" src="http://upload.wikimedia.org/wikipedia/en/thumb/0/03/Flag_of_Italy.svg/23px-Flag_of_Italy.svg.png" width="23" height="15" class="thumbborder"/></A></SPAN> <A href="http://en.wikipedia.org/wiki/Paolo_Casoli" title="Paolo Casoli">Paolo Casoli</A></TD>
                <TD>&nbsp;</TD>
               </TR>
              </TBODY>
             </TABLE>
        </body>
        </html>
        """
    soup = BeautifulSoup(html, 'html.parser')  # html.parser or lxml
    table_0 = soup.select('table')[0]
    fv = features_clustering(table_0)
    # TODO: finish all features first
    print(fv)
    assert fv == dict(C1_ratio_anchor=0.5, C1_ratio_images=0.0, C1_ratio_is_number=0.5, C2_ratio_anchor=0.5,
                      C2_ratio_images=0.5, C2_ratio_is_number=0.0, C3_ratio_anchor=0.0, C3_ratio_images=0.0,
                      C3_ratio_is_number=0.0, COL_NUM=3.0, TOKEN_champion=1.0, TOKEN_motorcycle=1.0, TOKEN_year=1.0)
