# -*- coding: utf-8 -*-

import csv
from zipfile import ZipFile
from StringIO import StringIO
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
import urlparse
import re
import lxml.html
import chinese_digit
from book_catlog_dao import BooKCatlogDao

class AlexaCallback:
    def __init__(self, max_urls=1000):
        self.max_urls = max_urls
        #http://m.biquge.biz/top/allvisit_1/
        #http://m.benbenwx.com/top/allvisit_1/
        #http://m.moliwenxue.com/top/allvisit_1/
        #http://m.boluoxs.com/top/allvisit_1/
        self.urls = []


        self.seed_url = 'http://www.junzige.la/'
        self.queue = MongoQueue()
        self.book_data = BooKCatlogDao()

    def __call__(self, url, html):
        # if url == self.seed_url:

        print url
        urls = []
        results = []
        queue = self.queue

        #get the matcher

        # matchStr = self.get_match(url)
        # print matchStr

        # filter for links matching our regular expression
        # and self.same_domain(link, seed_url)
        # for oneUrl in (self.normalize(self.seed_url, link) for link in self.get_links(html) if re.match(matchStr, link)):
        #     print oneUrl
        #     if oneUrl.endswith('_1_1/'):
        #         continue
        #     if self.same_domain(oneUrl, self.seed_url) and (oneUrl not in queue or queue[oneUrl] != 2):
        #         results.append(oneUrl)
        #
        # print len(results)
        # results = set(results)
        # print len(results)

        # html = lxml.html.tostring(html, pretty_print=True)
        tree = lxml.html.fromstring(html)

        bookname = tree.cssselect('div#maininfo div#info h1')[0].text_content()
        print bookname

        self.book_data.clear_content(bookname)

        contentNum = 0
        for t in tree.cssselect('div#list  dd a'):
            contentNum += 1
            record = {}

            if 'href' in t.attrib.keys():
                record['link'] = t.attrib['href']
            else:
                continue

            catlogName = t.text_content()
            record['name'] = catlogName[:]
            try:
                record['num'] = chinese_digit.getResultForDigit(catlogName[1: catlogName.find(' ') - 1])
            except:
                print catlogName[1: catlogName.find(' ') - 1] + 'can not parse number'
                record['num'] = 0


            self.book_data[bookname] = record

        print contentNum


        return None

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

    def get_match(self, url):
        list = url.split('/')

        list[:] = [item for item in list if item != '']
        matchStr = '/'
        for item in list[-3:] :
            if item.find('_') == -1:
                matchStr += item
            else:
                matchStr += item[0:item.find('_')]
            matchStr += '/'
        matchStr = matchStr[:-1] + '_'
        return matchStr