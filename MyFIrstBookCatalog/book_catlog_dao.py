try:
    import cPickle as pickle
except ImportError:
    import pickle
import zlib
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.binary import Binary


class BooKCatlogDao:
    """
    Wrapper around MongoDB to cache downloads

    >>> cache = MongoCache()
    >>> cache.clear()
    >>> url = 'http://example.webscraping.com'
    >>> result = {'html': '...'}
    >>> cache[url] = result
    >>> cache[url]['html'] == result['html']
    True
    >>> cache = MongoCache(expires=timedelta())
    >>> cache[url] = result
    >>> # every 60 seconds is purged http://docs.mongodb.org/manual/core/index-ttl/
    >>> import time; time.sleep(60)
    >>> cache[url] 
    Traceback (most recent call last):
     ...
    KeyError: 'http://example.webscraping.com does not exist'
    """
    def __init__(self, client=None, expires=timedelta(days=30)):
        """
        client: mongo database client
        expires: timedelta of amount of time before a cache entry is considered expired
        """
        # if a client object is not passed 
        # then try connecting to mongodb at the default localhost port 
        self.client = MongoClient('localhost', 27017, connect=False) if client is None else client
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
        self.db = self.client.cache
        self.db.books.create_index('timestamp', expireAfterSeconds=expires.total_seconds())

    def __contains__(self, url):
        try:
            self[url]
        except KeyError:
            return False
        else:
            return True
    
    def __getitem__(self, name):
        """Load value at this URL
        """
        record = self.db.books.find_one({'_id': name})
        if record:
            return record
            # return pickle.loads(zlib.decompress(record['result']))
        else:
            raise KeyError(name + ' does not exist')


    def __setitem__(self, name, results):
        """Save value for this URL
        """
        #record = {'result': result, 'timestamp': datetime.utcnow()}

        # num = results['num']
        # catlogname = results['name']
        # link = results['link']

        # no longer to remove duplications by num because the num is not unique
        # if (self.db.books.find_one({'_id': name, 'content.link': link}) == None):

        # record = {'content':{'num': num, 'catlogname': catlogname, 'link': link}}

        if self.catlog_count(name) != len(results):
            self.clear_content(name)
            self.db.books.update({'_id': name}, {'$push': {'content': {'$each': results}}}, upsert=True)


    def clear_content(self, name):
        record = {'content':[]}
        self.db.books.update({'_id': name}, {'$set': record}, upsert=True)

    def clear(self):
        self.db.books.drop()

    def catlog_count(self, name):
        temp = self.db.books.aggregate([
            {'$match': {'_id' : name}},
            {'$unwind': '$content' },
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ])

        if temp.alive:
            return temp.next()['count']
        else:
            return -1