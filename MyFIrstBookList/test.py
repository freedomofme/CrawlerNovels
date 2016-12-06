#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from datetime import datetime
from mongo_cache import MongoCache

# client = MongoClient('localhost', 27017)
# db = client.cache
#
# db.crawl_queue.remove()
# db.webpage.remove()
# db.books.remove()



client = MongoClient('localhost', 27017, connect=False)
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
db = client.cache
db.testing.create_index('timestamp', expireAfterSeconds=61)



# record = {'link': 'www1', 'author':'huang', 'timestamp': datetime.utcnow()}
# record = {'content':{'num': 2,'name':'屠戮黄巾'}}
# {'num': 2, 'name':'屠戮黄巾'}, {'num': 3, 'name':'赤壁之战'}, {'num': 4, 'name': '智取西川'}
# record = {}
# record['content'] = [8, '宜陵之战']
# record['content'].append([7, '宜陵之战'])
#
# db.testing.update({'_id': 'abc'}, {'$push': record}, upsert=True)

# print db.testing.find_one({'_id':'abc', 'content.num': 3})
record = {'content':{'num': 3,'name':'abcd'}}
db.testing.update({'_id': 'abc'}, {'$push': record}, upsert=True)
print db.testing.find_one({'_id':'abc', 'content.num': 3})

temp = db.books.aggregate([
     {'$match': {
        '_id' : '大主宰'
    }},

    # {'$project': {
    #     '$content' : ''
    # }},
    {'$unwind': '$content' },
    #
    {'$sort': {
        'content.num': -1
    }}
]

)

while temp.alive:
    print temp.next()





# url = "abc"
# html = "AAAA"
#
# db.webpage.insert({'url':url, 'html':html})
# print db.crawl_queue.find_one({"_id":"http://m.boluoxs.com/top/allvisit_1/"})

# cache = MongoCache()
# print cache["http://m.boluoxs.com/top/allvisit_1/"]


# print db.crawl_queue.find_one({'status':1})
# from mongo_queue import MongoQueue
#
# queue = MongoQueue()
# queue.repair()
# print db.crawl_queue.find_one({'status':1})
# for data in db.crawl_queue.find():
#     print data
