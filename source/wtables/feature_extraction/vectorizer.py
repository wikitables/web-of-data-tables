# -*- coding: utf-8 -*-

import re
from collections import defaultdict

from bs4 import BeautifulSoup

"""
Check old Java file that extracts the features:
https://bitbucket.org/emir-munoz/wikitables-code/src/c959c13f4a7898a90b993b55dd42c73f2c891a4c/data-tables-core/src/main/java/org/deri/data/tables/core/feature/extractor/FeaturesExtractorYData.java?at=master&fileviewer=file-view-default
https://bitbucket.org/emir-munoz/wikitables-code/src/6f6b0ecb438db547bf01d4de04e2674561ca5f81/data-tables-core/src/main/java/org/deri/data/tables/core/wikipedia/extractor/WikiTableTemplateExtractor.java?at=master&fileviewer=file-view-default
"""

TOKEN_OCCURRENCE_WEIGHT = 1


def clean_string(str):
    """ Takes an input string and applies different rules for cleansing.

    Parameters
    ----------
    str : String
        Input string to convert.

    Returns
    -------
    str
        Cleaned string.
    """
    _str = str.lower().strip()
    if len(_str) > 2 and (_str.endswith(':') or _str.endswith(',') or _str.endswith('.')):
        _str = _str[:len(_str) - 1]
    if '&' in _str:
        _str = _str.replace('&', ' and ').strip()
    _str = re.sub('\d', '', _str).strip()
    _str = re.sub('\\s+', '_', _str)
    _str = _str.replace('\u00A0', '').strip()
    return _str


def html_features(table_soup):
    """ Extracts HTML features from table.

    Parameters
    ----------
    table_soup : BeautifulSoup
        Input HTML table object.

    Returns
    -------
    defaultdict
        Dictionary of HTML features extracted.
    """
    fv_html = defaultdict(float)
    # MAX_ROWS(1), MAX_COLS(2), MAX_CELL_LENGTH(3), RATIO_COLSPAN(4), RATIO_ROWSPAN(5), DIST_TAGS(6), RATIO_TH(7),
	# RATIO_ANCHOR(8), RATIO_IMG(9), RATIO_INPUT(10)
    totalCellCounter = 0
    # RATIO_TH
    thCellCounter = 0
    # DIST_TAGS
    difTags = {}
    # RATIO_ANCHOR
    anchorCellCounter = 0
    # RATIO_IMG
    imgCellCounter = 0
    # RATIO_INPUT
    inputCellCounter = 0
    # RATIO_SELECT
    selectCellCounter = 0
    # RATIO_F
    fCellCounter = 0
    # RATIO_BR
    brCellCounter = 0

    max_rows = 0
    max_cols = 0
    max_cell_length = 0
    rows = table_soup.select('tr')
    max_rows = len(rows)

    for row_node in rows:
        columns = row_node.select('td')
        headers = row_node.select('th')

        if max_cols < len(columns):
            max_cols = len(columns)
        if max_cols < len(headers):
            max_cols = len(headers)

        # add the headers as columns
        if len(headers) > 0:
            columns.extend(headers)

        for cell_node in columns:
            print(cell_node)
            print(type(cell_node))
            print(cell_node.get_text())
            # if max_cell_length < len(cell_node.get_text()):
            #     max_cell_length = len(cell_node.get_text())

    return fv_html


def lexical_features(table_soup):
    """ Extracts lexical features from the table.

    Parameters
    ----------
    table_soup : BeautifulSoup
        Input HTML table object.

    Returns
    -------
    defaultdict
        Dictionary of lexical features extracted.
    """
    fv_lexical = defaultdict(int)
    totalCellCounter = 0
    # DIST_STRING
    diffString = {}
    # RATIO_COLON
    colonCellCounter = 0
    # RATIO_CONTAIN_NUMBER
    contNumberCellCounter = 0
    # RATIO_IS_NUMBER
    numberCellCounter = 0
    # RATIO_NONEMPTY
    nonemptyCellCounter = 0

    return fv_lexical


def bow_header_features(table_soup):
    """ Extracts bag-of-word features from the column headers.

    Parameters
    ----------
    table_soup : BeautifulSoup
        Input HTML table object.

    Returns
    -------
    defaultdict
        Dictionary of BoW features extracted.
    """
    fv_bow = defaultdict(int)
    headers = table_soup.select('tr > th')
    for header in headers:
        token = clean_string(header.get_text())
        key = 'TOKEN_' + token
        value = 1 * TOKEN_OCCURRENCE_WEIGHT
        fv_bow[key] += value
    return fv_bow


def features_clustering(table):
    fv_bow = bow_header_features(table)
    fv_html = html_features(table)
    fv_lexical = lexical_features(table)
    # concatenate dicts: http://treyhunner.com/2016/02/how-to-merge-dictionaries-in-python/
    return {**fv_bow, **fv_html, **fv_lexical}
