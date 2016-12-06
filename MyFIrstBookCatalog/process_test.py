# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from alexa_cb import AlexaCallback
from pymongo import MongoClient
import urlparse

def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
        """
    link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
    return urlparse.urljoin(seed_url, link)

def main(max_threads = 5):
    catlog_callback = AlexaCallback()
    cache = MongoCache()
    queue = MongoQueue()
    queue.repairFast()


    client = MongoClient('localhost', 27017, connect=False)
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
    db = client.cache
    cursor = db.books.find()

    urls = []
    while cursor.alive:
        temp = cursor.next()['link']
        temp = '/novel' + temp[5:-4] + '/'
        temp = normalize(catlog_callback.seed_url, temp)
        urls.append(temp)

    print urls[0]

    process_crawler(urls, scrape_callback=catlog_callback, cache=cache, max_threads=max_threads, timeout=30, host = 'www.junzige.la', user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36')


if __name__ == '__main__':
    max_threads = int(1)
    main(max_threads)

