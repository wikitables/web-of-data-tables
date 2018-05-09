#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import bz2
import itertools
import logging
import multiprocessing
import os
import re
import sys
import threading
import time
import urllib.parse
import warnings
from concurrent import futures
from functools import partial
from xml.etree import cElementTree

import requests
from requests.adapters import HTTPAdapter
from smart_open import smart_open
from urllib3.util.retry import Retry

# from bs4 import BeautifulSoup


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

The script takes several parts from [3,4], where the goal is to parse the Wikipedia XML dump and extract 
sequences of words.

[1] https://en.wikipedia.org/api/rest_v1/
[2] https://www.mediawiki.org/wiki/RESTBase
[3] https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/scripts/segment_wiki.py
[4] https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/corpora/wikicorpus.py
"""


def parse_cli_arguments(argv):
    def formatter(prog):
        return argparse.HelpFormatter(prog, max_help_position=100, width=200)

    argparser = argparse.ArgumentParser('Crawler of Wikipedia articles', formatter_class=formatter)

    nb_jobs_default = max(1, multiprocessing.cpu_count() - 1)

    # General options
    argparser.add_argument('--file', '-f', type=str, default=None,
                           help='Path to MediaWiki dump for read-only', required=True)
    argparser.add_argument('--output', '-o', type=str, default='./wikipedia_html',
                           help='Path to output directory')
    argparser.add_argument('--jobs', '-j', type=int, default=nb_jobs_default,
                           help='Number of jobs to run. Default (#cpus - 1)')
    argparser.add_argument('--workers', '-w', type=int, default=nb_jobs_default,
                           help='Number of workers in the crawler pool. Default (#cpus - 1)')
    argparser.add_argument('--debug', '-d', action='store_const', dest='loglevel',
                           const=logging.DEBUG, default=logging.WARNING, help='Show debug statements')
    argparser.add_argument('--verbose', '-v', action='store_const', dest='loglevel', const=logging.DEBUG,
                           help='Increase verbosity level')
    return argparser.parse_args(argv)


class WikipediaSpider(object):
    """ Sets gentle spider that respects download limits of an API.
    
    Based on ratelimit [1] functions.
    
    [1] https://github.com/tomasbasham/ratelimit/blob/master/ratelimit/decorators.py

    Parameters
    ----------
    calls : int
        Number of call in the `period`. Default 15
    period : int
        Seconds of the period. Default: 60
    clock : time.time
        Clock time object when started.
    raise_on_limit : bool
        Whether to raise an error when reaching the limit or not. Default: True
    output : str
        Path to the output folder.
    nb_pool : int, None
        Size of the pool for download.
    sleep : float
        Time set to sleep when the limit is reached. It can be a float. Default: 1.0
    """

    def __init__(self, calls=15, period=60, clock=None, raise_on_limit=True, output='.', nb_workers=None):
        self.clamped_calls = max(1, calls)
        self.period = period
        self.clock = clock
        self.raise_on_limit = raise_on_limit
        self.output = output
        # self.sleep = sleep
        self.nb_workers = nb_workers
        self.last_reset = clock()
        self.num_calls = 0
        # Add thread safety.
        self.lock = threading.RLock()
        # Get a session to the service
        self.session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.urls = None
        self.total_downloads = 0

    def set_urls(self, urls):
        self.urls = urls

    def get_all_html(self):
        # with futures.ThreadPoolExecutor(max_workers=self.nb_workers) as executor:
        #     executor.map(self.download_article, self.urls)
        with multiprocessing.Pool(processes=self.nb_workers) as executor:
            executor.map(self.download_article, self.urls)
            executor.join()

    def get_html(self, article_title):
        """ Retrieve the HTML text from the API.

        A thread is created to handle each request.

        Parameters
        ----------
        article_title : str
            Article title.
        """
        with self.lock:
            period_remaining = self.__period_remaining()
            # If the time window has elapsed then reset.
            if period_remaining <= 0:
                self.num_calls = 0
                self.last_reset = self.clock()

            # Increase the number of attempts to call the function.
            self.num_calls += 1

            # If the number of attempts to call the function exceeds the
            # maximum then sleep the thread for half a second.
            if self.num_calls > self.clamped_calls:
                logger.info('Sleeping for {} second(s)'.format(period_remaining))
                time.sleep(period_remaining)
                return

            try:
                thread = threading.Thread(target=self.download_article, args=(article_title,))
                # thread.daemon = True
                thread.start()
                # thread.join()
                return
            except (KeyboardInterrupt, SystemExit):
                logger.info('Received keyboard interrupt, quitting threads.')
                sys.exit()

        # if threading.active_count() >= self.num_calls:
        #     logger.info('Number of active threads too big, sleeping for a while.')
        #     time.sleep(self.sleep)

    def download_article2(self, article_title):
        print(self.num_calls, article_title)
        time.sleep(0.1)

    def download_article(self, article_title):
        """ Method to query the API and retrieve the HTML text.

        The HTML is then saved to disk compressed using bz2.

        Parameters
        ----------
        article_title : str
            Article title.
        """
        api_url = '{0}page/html/{1}?redirect=false'.format(api_url_base, article_title)
        # query the API
        response = self.session.get(api_url, headers=headers)
        if response.status_code == 200:
            # return response.content.decode('utf-8')
            html_text = response.content.decode('utf-8')

            # fix the base and css
            # soup = BeautifulSoup(html_text, 'html.parser')
            # soup.base['href'] = 'http://en.wikipedia.org/wiki/'
            # print(soup.base)

            # links stay relative https://stackoverflow.com/a/44002598
            # https://codereview.stackexchange.com/questions/100490/extracting-and-normalizing-urls-in-an-html-document
            # http://www.compjour.org/warmups/govt-text-releases/extracting-absolute-wh-press-briefings-urls/
            # links = soup.find_all('a')
            # print(links)

            with bz2.BZ2File('{0}/{1}.html.bz2'.format(self.output, article_title), mode='w') as bz2f:
                bz2f.write(str.encode(html_text))

            self.total_downloads += 1
            if self.total_downloads % 200 == 0:  # 100000
                logger.info("processed #%d articles (at %r now)", self.total_downloads, article_title)
        else:
            warnings.warn('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url))
        # time.sleep(0.1)

    def __period_remaining(self):
        """ Return the period remaining for the current rate limit window.

        Returns
        -------
        float
            The remaining period.
        """
        elapsed = self.clock() - self.last_reset
        return self.period - elapsed


class WikiParser(object):
    """ Treats a wikipedia articles dump (<LANG>wiki-<YYYYMMDD>-pages-articles.xml.bz2
    or <LANG>wiki-latest-pages-articles.xml.bz2) as a (read-only) corpus.

    The documents are extracted on-the-fly, so that the whole (massive) dump can stay compressed on disk.

    Parameters
    ----------
    wiki_dump : object
        File object of the Wikipedia XML dump.
    min_article_character : int
        Minimum number of characters to consider in one article.
    nb_jobs : int
        Number of jobs to use.
    """

    def __init__(self, wiki_dump, min_article_character=200, nb_jobs=None):
        self.wiki_dump = wiki_dump
        self.nb_jobs = nb_jobs
        self.min_article_character = min_article_character
        self.nb_articles = 0

    def get_articles(self):
        """ Parse the XML dump and retrieve the titles and sections.

        Returns
        -------
        Generator
            Pair of title and sections of each article.
        """
        skipped_redirect = 0
        skipped_disambiguation = 0
        skipped_namespace = 0
        skipped_length = 0
        total_articles = 0
        xml_pages = parse_xml_pages(self.wiki_dump)
        # http://chriskiehl.com/article/parallelism-in-one-line/
        pool = multiprocessing.Pool(self.nb_jobs)

        # process the whole XML dump file in small chunks
        for group in chunksize_groups(xml_pages, chunksize=10 * self.nb_jobs):
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
                # filter disambiguation pages
                if article_title.endswith('(disambiguation)'):
                    skipped_disambiguation += 1
                    continue
                # filter stubs (incomplete, very short articles)
                if sum(len(body.strip()) for (_, body) in sections) < self.min_article_character:
                    skipped_length += 1
                    continue

                total_articles += 1
                yield (article_title, sections)

        logger.info('finished processing %i articles '
                    '(skipped %i redirects, %i disambiguation, %i stubs, %i ignored namespaces)',
                    total_articles, skipped_redirect, skipped_disambiguation, skipped_length, skipped_namespace)
        pool.terminate()
        self.nb_articles = total_articles


def get_namespace(tag):
    """ Return the namespace of a tag.

    Parameters
    ----------
    tag : str
        Tag string.

    Returns
    -------
    str
        Identified namespace.
    """
    m = re.match("^{(.*?)}", tag)
    namespace = m.group(1) if m else ""
    if not namespace.startswith("http://www.mediawiki.org/xml/export-"):
        raise ValueError("%s not recognized as MediaWiki dump namespace" % namespace)
    return namespace


def parse_xml_pages(xml_dump):
    """ Parse an XML page from the dump.

    Parameters
    ----------
    xml_dump : object
        File object of the Wikipedia XML dump.

    Returns
    -------
    Generator
        Iterator over page elements as string.
    """
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
    """ Segment a given XML page.

    Parameters
    ----------
    page_xml : str
        XML page as string.

    Returns
    -------
    str
        Page title.
    list
        Page sections.
    """
    elem = cElementTree.fromstring(page_xml)
    namespace = get_namespace(elem.tag)
    ns_mapping = {"ns": namespace}
    text_path = "./{%(ns)s}revision/{%(ns)s}text" % ns_mapping
    title_path = "./{%(ns)s}title" % ns_mapping
    # ns_path = "./{%(ns)s}ns" % ns_mapping
    lead_section_heading = "Introduction"
    top_level_heading_regex = r"\n==[^=].*[^=]==\n"
    top_level_heading_regex_capture = r"\n==([^=].*[^=])==\n"

    title = elem.find(title_path).text
    text = elem.find(text_path).text
    # ns = elem.find(ns_path).text

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


def chunksize_groups(iterable, chunksize):
    """ Chunk the stream of pages in groups of a given size.

    Parameters
    ----------
    iterable : Iterable
        Stream of XML parts.
    chunksize : int
        Size of the chunk to use.

    Returns
    -------
    Generator
        Iterator over the chunk groups.
    """
    it = iter(iterable)
    while True:
        wrapped_chunk = [list(itertools.islice(it, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        yield wrapped_chunk.pop()


def parse_all_articles(xml_dump, min_article_character, nb_jobs=None):
    """ Parse the Wikipedia XML dump and retrieve page by page.

    Parameters
    ----------
    xml_dump : str
        Path to the Wikipedia XML dump file.
    min_article_character : int
        Minimum number of characters to consider in one article.
    nb_jobs : int
        Number of jobs to use.

    Returns
    -------
    Generator
        Iterator for the filtered articles.
    """
    with smart_open(xml_dump, 'rb') as dump:
        parser_dump = WikiParser(dump, min_article_character, nb_jobs)
        wiki_articles_stream = parser_dump.get_articles()
        for article in wiki_articles_stream:
            yield article[0]


def parse_articles(xml_dump, output, min_article_character=200, nb_jobs=None, nb_workers=None):
    """ Parse the Wikipedia dump, retrieve the articles' title and request the HTML
    text from the Wikipedia API.

    Parameters
    ----------
    xml_dump : str
        Path to the Wikipedia XML dump file.
    output : str
        Path to the output directory.
    min_article_character : int
        Minimum number of characters to consider in one article.
    nb_jobs : int, None
        Number of jobs to use.
    nb_workers : int, None
        Size of the pool for download.
    """
    article_stream = parse_all_articles(xml_dump, min_article_character, nb_jobs)
    # create output directory if does not exist
    output_path = os.path.abspath(output)
    os.makedirs(output_path, exist_ok=True)
    # create a spider that respects the crawling rules:
    # max 200 requests per second
    now = time.monotonic if hasattr(time, 'monotonic') else time.time
    spider = WikipediaSpider(calls=199, period=1, clock=now, raise_on_limit=True,
                             output=output_path, nb_workers=nb_workers)
    spider.set_urls(article_stream)
    spider.get_all_html()
    # max_count = 2
    # for idx, article in enumerate(article_stream):
    #     article_title = article[0]
    #     # print('TITLE=',article_title)
    #     spider.get_html(article_title)
    #     # print(html_text)
    #     # if idx > max_count:
    #     #    break
    #     if (idx + 1) % 100000 == 0:
    #         logger.info("processed #%d articles (at %r now)", idx + 1, article_title)


def main(argv):
    """
    $ python parse_wiki_dump.py --file enwiki-20180420-pages-articles.xml.bz2 --jobs 1
    """
    # User parameters
    args = parse_cli_arguments(argv)
    dump_file = args.file
    output_dir = args.output
    nb_jobs = args.jobs
    nb_workers = args.workers
    logger.info('running %s (jobs=%d, pool=%d)', ' '.join(sys.argv), nb_jobs, nb_workers)
    parse_articles(dump_file, output=output_dir, nb_jobs=nb_jobs, nb_workers=nb_workers)


if __name__ == "__main__":
    main(sys.argv[1:])
