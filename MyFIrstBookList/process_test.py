# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from alexa_cb import AlexaCallback


def main(max_threads = 5):
    scrape_callback = AlexaCallback()
    cache = MongoCache()
    queue = MongoQueue()
    queue.repairFast()

    process_crawler(scrape_callback.seed_url, scrape_callback=scrape_callback, cache=cache, max_threads=max_threads, timeout=30, host = 'm.junzige.la', user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36')


if __name__ == '__main__':
    max_threads = int(1)
    main(max_threads)
