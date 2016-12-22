# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from alexa_cb import AlexaCallback
from datetime import datetime
import time
import urlparse

def main(max_threads = 5):
    scrape_callback = AlexaCallback()
    cache = MongoCache()
    queue = MongoQueue()

    urls = []
    temple = scrape_callback.seed_url[0: -2]
    for i in range(1, 1189, 1):
        urls.append(temple + str(i)+'/')


    while True:
        now = datetime.now()
        if now.hour < 3 or now.hour > 12:
            queue.repairFast()
            process_crawler(urls, scrape_callback=scrape_callback, cache=cache, max_threads=max_threads, timeout=30, host = urlparse.urlparse(scrape_callback.seed_url).netloc, user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36')
        else:
            print 'pass:' + str(now)
            pass
        time.sleep(3600)


if __name__ == '__main__':
    max_threads = int(4)
    main(max_threads)
