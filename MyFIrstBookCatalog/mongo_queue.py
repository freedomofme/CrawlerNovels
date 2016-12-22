from datetime import datetime, timedelta
from pymongo import MongoClient, errors


class MongoQueue:
    """
    >>> timeout = 1
    >>> url = 'http://example.webscraping.com'
    >>> q = MongoQueue(timeout=timeout)
    >>> q.clear() # ensure empty queue
    >>> q.push(url) # add test URL
    >>> q.peek() == q.pop() == url # pop back this URL
    True
    >>> q.repair() # immediate repair will do nothin
    >>> q.pop() # another pop should be empty
    >>> q.peek() 
    >>> import time; time.sleep(timeout) # wait for timeout
    >>> q.repair() # now repair will release URL
    Released: test
    >>> q.pop() == url # pop URL again
    True
    >>> bool(q) # queue is still active while outstanding
    True
    >>> q.complete(url) # complete this URL
    >>> bool(q) # queue is not complete
    False
    """

    # possible states of a download
    OUTSTANDING, PROCESSING, COMPLETE = range(3)
    RETRY_TIMES = 3

    def __init__(self, client=None, timeout=300, expires=timedelta(hours=6)):
        """
        host: the host to connect to MongoDB
        port: the port to connect to MongoDB
        timeout: the number of seconds to allow for a timeout
        """
        self.client = MongoClient(connect=False) if client is None else client
        self.db = self.client.cache
        #Huang clear task queue
        # self.db.cotlog_queue.remove()
        self.timeout = timeout
        self.db.cotlog_queue.create_index('timestamp', expireAfterSeconds=expires.total_seconds())

    def __nonzero__(self):
        """Returns True if there are more jobs to process
        """
        record = self.db.cotlog_queue.find_one(
            {'status': {'$ne': self.COMPLETE}} 
        )
        return True if record else False

    def __contains__(self, url):
        try:
            self[url]
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, url):
        """Load value at this URL
        """
        record = self.db.cotlog_queue.find_one({'_id': url})
        if record:
            return record['status']
        else:
            raise KeyError(url + ' does not exist')


    def push(self, url):
        """Add new URL to queue if does not exist
        """
        try:
            self.db.cotlog_queue.insert({'_id': url, 'status': self.OUTSTANDING})
        except errors.DuplicateKeyError as e:
            pass # this is already in the queue

    def pushAll(self, urls):
        """Add new URL to queue if does not exist
        """
        records = []
        for url in urls:
            records.append({'_id': url, 'status': self.OUTSTANDING})

        try:
            self.db.cotlog_queue.insert(records)
        except errors.DuplicateKeyError as e:
            pass # this is already in the queue

    def pop(self):
        """Get an outstanding URL from the queue and set its status to processing.
        If the queue is empty a KeyError exception is raised.
        """
        record = self.db.cotlog_queue.find_and_modify(
            query={'status': self.OUTSTANDING}, 
            update={'$set': {'status': self.PROCESSING, 'timestamp': datetime.now()}}
        )
        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError()

    def peek(self):
        record = self.db.cotlog_queue.find_one({'status': self.OUTSTANDING})
        if record:
            return record['_id']

    def complete(self, url):
        self.db.cotlog_queue.update({'_id': url}, {'$set': {'status': self.COMPLETE}})

    def repush(self, url):
        record = self.db.cotlog_queue.find_one({'_id': url})

        if (record.has_key('retry')):
            print('has_key(\'retry\')')
            if (record['retry'] <= self.RETRY_TIMES):
                self.db.cotlog_queue.update({'_id': url}, {'$set': {'status': self.OUTSTANDING, 'retry':record['retry'] + 1}})
            else:
                self.db.cotlog_queue.update({'_id': url}, {'$set': {'status': self.COMPLETE, 'retry': 999}})
        else:
            print('not has_key(\'retry\')')
            self.db.cotlog_queue.update({'_id': url}, {'$set': {'status': self.OUTSTANDING, 'retry': 1}})


    def repair(self):
        """Release stalled jobs
        """
        print "repair"
        record = self.db.cotlog_queue.find_and_modify(
            query={
                'timestamp': {'$lt': datetime.now() - timedelta(seconds=self.timeout)},
                'status': {'$ne': self.COMPLETE}
            },
            update={'$set': {'status': self.OUTSTANDING}}
        )
        if record:
            print 'Released:', record['_id']

    def repairFast(self):
        """Release stalled jobs
        """
        print "repairFast"
        record = self.db.cotlog_queue.find_and_modify(
            query={
                'timestamp': {'$lt': datetime.now() - timedelta(seconds=0)},
                'status': {'$ne': self.COMPLETE}
            },
            update={'$set': {'status': self.OUTSTANDING}}
        )
        if record:
            print 'Released:', record['_id']

    def clear(self):
        self.db.cotlog_queue.drop()
