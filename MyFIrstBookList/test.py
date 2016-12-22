#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from datetime import datetime
import time
import urlparse
from mongo_cache import MongoCache

# client = MongoClient('localhost', 27017)
# db = client.cache
#
# db.crawl_queue.remove()
# db.webpage.remove()
# db.books.remove()

text = '''<span class="">���ߣ�<a href="/author/½˫��">½˫��</a></span>
                <span class="">    "/>
    <meta property=
...</span>
'''

print text.replace('''"/>
    <meta property=''', '')


print urlparse.urlparse('http://m.boluoxs.com/top/allvisit_400/').netloc
client = MongoClient('localhost', 27017, connect=False)
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
db = client.cache
result = db.books.find({})
while result.alive :
    print result.next()['intro']




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
record = [{'num': 4,'name':'ddd'}, {'num': 5,'name':'eee'}]
db.testing.update({'_id': 'abc'}, {'$push': {'content': {'$each': record}}}, upsert=True)
print db.testing.find_one({'_id':'abc', 'content.num': 3})


result = db.books.find({})



temp = db.books.aggregate([
     {'$match': {
        '_id' : '大主宰'
    }},

    # {'$project': {
    #     '$content' : ''
    # }},
    {'$unwind': '$content' },
    #  {'$match': {
    #     '_id' : '真武世界'
    # }},
    #
    # {'$sort': {
    #     'content.num': -1
    # }}
    # { '$group': { '_id': '$content.link', 'count': { '$sum': 1 } } }

    {"$group": {"_id": None, "total": {"$sum": 1}}}
]

)

num = 0

if temp.alive:
    print temp.next()['total']
    num +=1
print num





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
