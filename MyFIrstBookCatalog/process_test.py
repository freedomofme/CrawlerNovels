# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from alexa_cb import AlexaCallback
from pymongo import MongoClient
import urlparse
from datetime import datetime
import time

def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
        """
    link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
    return urlparse.urljoin(seed_url, link)

def main(max_threads = 5):
    catlog_callback = AlexaCallback()
    cache = MongoCache()
    queue = MongoQueue()


    client = MongoClient('localhost', 27017, connect=False)
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
    db = client.cache
    cursor = db.books.find()

    urls = []
    while cursor.alive:
        temp = cursor.next()
        temp = temp['link']

        if urlparse.urlparse(catlog_callback.seed_url).netloc == 'www.junzige.la':
            temp = '/novel' + temp[5:-4] + '/'
            temp = normalize(catlog_callback.seed_url, temp)
        elif urlparse.urlparse(catlog_callback.seed_url).netloc == 'www.boluoxs.com':
            temp = 'http://www.boluoxs.com/biquge/0/' + temp[temp.rfind('/') + 1 :temp.rfind('.')] + '/'

        print temp
        urls.append(temp)

    print urls[0]

    while True:
        now = datetime.now()

        if now.hour < 3 or now.hour > 12:
            queue.repairFast()
            process_crawler(urls, scrape_callback=catlog_callback, cache=cache, max_threads=max_threads, timeout=30, host = urlparse.urlparse(catlog_callback.seed_url).netloc, user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36')
            # every time finished, clear the job queue
            queue.clear()
        else:
            print 'pass:' + str(now)
            pass
        time.sleep(3600)



if __name__ == '__main__':
    max_threads = int(5)
    main(max_threads)

