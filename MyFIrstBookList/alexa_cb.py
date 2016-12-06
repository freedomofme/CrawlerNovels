# -*- coding: utf-8 -*-

import csv
from zipfile import ZipFile
from StringIO import StringIO
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
import urlparse
import re
import lxml.html
from book_list_dao import BooKListDao

class AlexaCallback:
    def __init__(self, max_urls=1000):
        self.max_urls = max_urls
        #http://m.biquge.biz/top/allvisit_1/
        #http://m.benbenwx.com/top/allvisit_1/
        #http://m.moliwenxue.com/top/allvisit_1/
        #http://m.boluoxs.com/top/allvisit_1/
        self.seed_url = 'http://m.junzige.la/top/allvisit_1/'
        self.queue = MongoQueue()
        self.book_data = BooKListDao()

    def __call__(self, seed_url, url, html):
        # if url == self.seed_url:

        urls = []
        results = []
        queue = self.queue

        # filter for links matching our regular expression
        # and self.same_domain(link, seed_url)
        for oneUrl in (self.normalize(seed_url, link) for link in self.get_links(html) if re.search('allvisit_', link)):
            print oneUrl
            if self.same_domain(oneUrl, seed_url) and (oneUrl not in queue or queue[oneUrl] != 2):
                results.append(oneUrl)


        # html = lxml.html.tostring(html, pretty_print=True)
        tree = lxml.html.fromstring(html)

        for t in tree.cssselect('ul.xbk'):
            book = []
            name = None
            for index, tag in enumerate(t.cssselect('li.tjxs > span')):
                if index == 0:
                    book.append(tag.cssselect('a')[0].attrib['href'])
                    name = tag.cssselect('a')[0].text_content()
                    # print name
                    # print tag.cssselect('a')[0].text_content()
                    # print tag.cssselect('a')[0].attrib['href']
                if index == 1:
                    book.append(tag.cssselect('a')[0].text_content())
                    book.append(tag.cssselect('a')[0].attrib['href'])
                    # print tag.cssselect('a')[0].text_content()
                    # print tag.cssselect('a')[0].attrib['href']
                if index == 2:
                    book.append(tag.text_content())
                    # print tag.text_content()
                if index == 3:
                    book.append(tag.cssselect('i')[0].text_content())
                    # print tag.cssselect('i')[0].text_content()
            if name is not None:
                self.book_data[name] = book


        return results

    def normalize(self, seed_url, link):
        """Normalize this URL by removing hash and adding domain
        """
        link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
        return urlparse.urljoin(seed_url, link)

    def get_links(self, html):
        """Return a list of links from html
        """
        # a regular expression to extract all links from the webpage
        webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
        # list of all links from the webpage
        return webpage_regex.findall(html)

    def same_domain(self, url1, url2):
        """Return True if both URL's belong to same domain
        """
        return urlparse.urlparse(url1).netloc == urlparse.urlparse(url2).netloc