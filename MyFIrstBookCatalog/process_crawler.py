import time
import urlparse
import threading
import multiprocessing
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from downloader import Downloader

SLEEP_TIME = 1


def threaded_crawler(seed_url, delay=2, cache=None, scrape_callback=None, user_agent=None, host=None, proxies=None, num_retries=1, max_threads=10, timeout=20):
    """Crawl using multiple threads
    """
    # the queue of URL's that still need to be crawled
    print 'threaded_crawler'
    crawl_queue = MongoQueue()
    # crawl_queue.clear()

    if isinstance(seed_url,list):
        crawl_queue.pushAll(seed_url)
    else:
        crawl_queue.push(seed_url)


    D = Downloader(cache=cache, delay=delay, user_agent=user_agent, host = host,proxies=proxies, num_retries=num_retries, timeout=timeout)

    def process_queue():
        while True:
            # keep track that are processing url
            try:
                url = crawl_queue.pop()
            except KeyError:
                # currently no urls to process
                break
            else:
                html = D(url)
                if scrape_callback:
                    try:
                        links = scrape_callback(url, html) or []
                        crawl_queue.complete(url)
                    except Exception as e:
                        crawl_queue.repush(url)
                        print 'Error in callback for: {}: {}'.format(url, e)
                        print 'repush this link {} to crawl queue'.format(url)
                    else:
                        for link in links:
                            # add this new link to queue
                            crawl_queue.push(normalize(scrape_callback.seed_url, link))



    # wait for all download threads to finish
    threads = []
    while threads or crawl_queue:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue.peek():
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)
        time.sleep(SLEEP_TIME)


def process_crawler(args, **kwargs):
    startTime = time.time()

    num_cpus = multiprocessing.cpu_count()
    num_cpus = max(num_cpus, 2)
    #pool = multiprocessing.Pool(processes=num_cpus)
    print 'Starting {} processes'.format(num_cpus)
    processes = []
    for i in range(num_cpus):
        p = multiprocessing.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        #parsed = pool.apply_async(threaded_link_crawler, args, kwargs)
        p.start()
        processes.append(p)
    # wait for processes to complete
    for p in processes:
        p.join()

    endTime = time.time()
    costTime = int(endTime - startTime)

    print str.format("{}hours, {}minutes", costTime /3600, costTime % 3600 / 60)

def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
    """
    link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
    return urlparse.urljoin(seed_url, link)
