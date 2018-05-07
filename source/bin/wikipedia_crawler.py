#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import time
import requests
import logging
import threading
import multiprocessing
import itertools
import re
import sys
import bz2
from math import floor
from xml.etree import cElementTree
from functools import partial
from smart_open import smart_open
from bs4 import BeautifulSoup


__author__ = 'emunoz'

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
logger = logging.getLogger('wodt')

"""Table markup"""
IGNORED_NAMESPACES = [
    'Wikipedia', 'Category', 'File', 'Portal', 'Template',
    'MediaWiki', 'User', 'Help', 'Book', 'Draft', 'WikiProject',
    'Special', 'Talk'
]
"""MediaWiki namespaces [2]_ that ought to be ignored.
References
----------
.. [2] https://www.mediawiki.org/wiki/Manual:Namespace
"""

api_url_base = 'https://en.wikipedia.org/api/rest_v1/'
headers = {'Content-Type': 'text/html',
           'User-Agent': 'HTML Spider (emir@emunoz.org)',
           'Accept': 'text/html; charset=utf-8; profile="https://www.mediawiki.org/wiki/Specs/HTML/1.6.1"'}

"""
This script is made to crawl the Wikipedia APIs [1,2] and retrieve the HTML text as a (compressed) file 
for all articles whose content is not for disambiguation or redirect.

An example on how to use the API is:
$ curl -X GET --header 'Accept: text/html; charset=utf-8; profile="https://www.mediawiki.org/wiki/Specs/HTML/1.6.1"' \
'https://en.wikipedia.org/api/rest_v1/page/html/Isaac%20Alb%C3%A9niz'

The script takes several parts from [3,4], where the goal is to parse the Wikipedia XML dump and extract sequences of words.

[1] https://en.wikipedia.org/api/rest_v1/
[2] https://www.mediawiki.org/wiki/RESTBase
[3] https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/scripts/segment_wiki.py
[4] https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/corpora/wikicorpus.py
"""


def parse_cli_arguments(argv):
    def formatter(prog):
        return argparse.HelpFormatter(prog, max_help_position=100, width=200)

    argparser = argparse.ArgumentParser('Parser of Wikipedia Tables', formatter_class=formatter)
    
    nb_jobs_default = max(1, multiprocessing.cpu_count() - 1)

    # General options
    argparser.add_argument('--file', '-f', type=str, default=None,
                           help='Path to MediaWiki dump for read-only', required=True)
    argparser.add_argument('--jobs', '-j', type=int, default=nb_jobs_default,
                           help='Number of jobs to run. Default (#cpus - 1)')
    argparser.add_argument('--debug', '-d', action='store_const', dest='loglevel',
                           const=logging.DEBUG, default=logging.WARNING, help='Show debug statements')
    argparser.add_argument('--verbose', '-v', action='store_const', dest='loglevel', const=logging.DEBUG,
                           help='Increase verbosity level')
    return argparser.parse_args(argv)


class WikipediaSpider(object):
    """ Sets gentle spider that respects download limits of an API.
    
    Based on ratelimit [1] functions.
    
    [1] https://github.com/tomasbasham/ratelimit/blob/master/ratelimit/decorators.py
    """
    def __init__(self, calls=15, period=60, clock=None, raise_on_limit=True):
        self.clamped_calls = max(1, calls)
        self.period = period
        self.clock = clock
        self.raise_on_limit = raise_on_limit
        self.last_reset = clock()
        self.num_calls = 0
        # Add thread safety.
        self.lock = threading.RLock()

    def get_html(self, article_title):
        thread = threading.Thread(target=self.download_article, args=(article_title,))
        thread.daemon = True
        thread.start()
    
    def download_article(self, article_title):
        api_url = '{0}page/html/{1}?redirect=false'.format(api_url_base, article_title)
        
        period_remaining = self.__period_remaining()
        # If the time window has elapsed then reset.
        if period_remaining <= 0:
            self.num_calls = 0
            self.last_reset = self.clock()

        # Increase the number of attempts to call the function.
        self.num_calls += 1
        
        # If the number of attempts to call the function exceeds the
        # maximum then raise an exception.
        if self.num_calls > self.clamped_calls:
            logger.info('Sleeping for half a second')
            time.sleep(0.5)

        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:
            # print(response.headers)
            #return response.content.decode('utf-8')
            html_text = response.content.decode('utf-8')
            
            # fix the base and css
            # soup = BeautifulSoup(html_text, 'html.parser')
            # soup.base['href'] = 'http://en.wikipedia.org/wiki/'
            # print(soup.base)
        
            # links stay relative https://stackoverflow.com/a/44002598
            # https://codereview.stackexchange.com/questions/100490/extracting-and-normalizing-urls-in-an-html-document
            # links = soup.find_all('a')
            # print(links)

            with bz2.BZ2File('./wikipedia_html/{0}.html.bz2'.format(article_title), mode='w') as bz2f:
                bz2f.write(str.encode(str(soup)))
            
        else:
            print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url))
            # return None
            
    def __period_remaining(self):
        """
        Return the period remaining for the current rate limit window.
        :return: The remaing period.
        :rtype: float
        """
        elapsed = self.clock() - self.last_reset
        return self.period - elapsed


def get_namespace(tag):
    m = re.match("^{(.*?)}", tag)
    namespace = m.group(1) if m else ""
    if not namespace.startswith("http://www.mediawiki.org/xml/export-"):
        raise ValueError("%s not recognized as MediaWiki dump namespace" % namespace)
    return namespace


def parse_xml_pages(xml_dump):
    elems = (elem for _, elem in cElementTree.iterparse(xml_dump, events=("end",)))
    elem = next(elems)
    namespace = get_namespace(elem.tag)
    ns_mapping = {"ns": namespace}
    page_tag = "{%(ns)s}page" % ns_mapping
    
    for elem in elems:
        # only retrieve pages
        if elem.tag == page_tag:
            yield cElementTree.tostring(elem)
            elem.clear()


def segment(page_xml):
    elem = cElementTree.fromstring(page_xml)
    namespace = get_namespace(elem.tag)
    ns_mapping = {"ns": namespace}
    text_path = "./{%(ns)s}revision/{%(ns)s}text" % ns_mapping
    title_path = "./{%(ns)s}title" % ns_mapping
    ns_path = "./{%(ns)s}ns" % ns_mapping
    lead_section_heading = "Introduction"
    top_level_heading_regex = r"\n==[^=].*[^=]==\n"
    top_level_heading_regex_capture = r"\n==([^=].*[^=])==\n"
    
    title = elem.find(title_path).text
    text = elem.find(text_path).text
    ns = elem.find(ns_path).text

    # currently filtering only the first paragraph
    if text is not None:
        section_contents = re.split(top_level_heading_regex, text)
        section_headings = [lead_section_heading] + re.findall(top_level_heading_regex_capture, text)
        section_headings = [heading.strip() for heading in section_headings]
        assert len(section_contents) == len(section_headings)
    else:
        section_contents = []
        section_headings = []
	
    sections = list(zip(section_headings, section_contents))

    return title, sections


def chunksize_groups(iterable, chunksize, maxsize=0):
    it = iter(iterable)
    while True:
        wrapped_chunk = [list(itertools.islice(it, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        yield wrapped_chunk.pop()


class WikiParser():
    """ Treats a wikipedia articles dump (<LANG>wiki-<YYYYMMDD>-pages-articles.xml.bz2
    or <LANG>wiki-latest-pages-articles.xml.bz2) as a (read-only) corpus.
    The documents are extracted on-the-fly, so that the whole (massive) dump can stay compressed on disk.
    """
    def __init__(self, wiki_dump, min_article_character=200, nb_jobs=None):
        self.wiki_dump = wiki_dump
        self.nb_jobs = nb_jobs
        self.min_article_character = min_article_character
        self.nb_articles = 0
	
    def get_articles(self):
        skipped_redirect = 0
        skipped_disambiguation = 0
        skipped_namespace = 0
        skipped_length = 0
        total_articles = 0
        xml_pages = parse_xml_pages(self.wiki_dump)
        pool = multiprocessing.Pool(self.nb_jobs)
        
	    # process the whole XML dump file in small chunks
        for group in chunksize_groups(xml_pages, chunksize=10 * self.nb_jobs, maxsize=1):
            for article in pool.imap(partial(segment), group):
                article_title, sections = article[0], article[1]
	            # filtering of articles such as redirect
                # filter non-articles
                if any(article_title.startswith(ignore + ':') for ignore in IGNORED_NAMESPACES):
                    skipped_namespace += 1
                    continue
                # filter redirect
                if not sections or sections[0][1].lstrip().lower().startswith("#redirect"):
                    skipped_redirect += 1
                    continue
                if article_title.endsswith('(disambiguation)'):
                    skipped_disambiguation += 1
                    continue
                # filter stubs (incomplete, very short articles)
                if sum(len(body.strip()) for (_, body) in sections) < self.min_article_character:
                    skipped_length += 1
                    continue
                # print('TITLE=',article_title)
                # print('SEC=',sections[0][1])
                total_articles += 1
                yield (article_title, sections)
	
        logger.info(
            "finished processing %i articles with %i sections (skipped %i redirects, %i disambiguation, %i stubs, %i ignored namespaces)",
            total_articles, total_sections, skipped_redirect, skipped_disambiguation, skipped_length, skipped_namespace)
        pool.terminate()
        self.nb_articles = total_articles


def parse_all_articles(xml_dump_path, min_article_character, nb_jobs=None):
    with smart_open(xml_dump_path, 'rb') as dump:
        parser_dump = WikiParser(dump, min_article_character, nb_jobs)
        wiki_articles_stream = parser_dump.get_articles()
        for article in wiki_articles_stream:
            yield article


def parse_articles(xml_dump_path, min_article_character=200, nb_jobs=None):
    article_stream = parse_all_articles(xml_dump_path, min_article_character, nb_jobs)
    max_count = 2
    counter = 1
    
    # create a spider that respects the crawling rules
    now = time.monotonic if hasattr(time, 'monotonic') else time.time
    # period in seconds
    spider = WikipediaSpider(calls=200, period=1, clock=now, raise_on_limit=True)
    for idx, article in enumerate(article_stream):
        article_title = article[0]
        # print('TITLE=',article_title)
        html_text = spider.get_html(article_title)
        # print(html_text)
        
        #counter += 1
        #if counter > max_count:
        #    break
        
        if (idx + 1) % 1000 == 0:  # 100000
            logger.info("processed #%d articles (at %r now)", idx + 1, article_title)


def main(argv):
    """
    $ python parse_wiki_dump.py --file enwiki-20180401-pages-articles1.xml-p10p30302.bz2 --jobs 1
    """
    # User parameters
    args = parse_cli_arguments(argv)
    dump_file = args.file
    nb_jobs = args.jobs
    logger.info('running %s (jobs=%d)', ' '.join(sys.argv), nb_jobs)
    
    parse_articles(dump_file, nb_jobs=nb_jobs)

if __name__ == "__main__":
    main(sys.argv[1:])
