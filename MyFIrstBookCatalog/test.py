#!/usr/bin/env python
# coding: utf-8
from pymongo import MongoClient
from bson.son import SON
from datetime import datetime
from mongo_cache import MongoCache

# client = MongoClient('localhost', 27017)
# db = client.cache
#
# db.crawl_queue.remove()
# db.webpage.remove()
# db.books.remove()
import sys
import time

list = [1,2,3]

name = '第一章 大 厦'
print sys.getdefaultencoding()
print type(name)
print isinstance(name, str)
# http://blog.sina.com.cn/s/blog_6ce9e8870101gqzt.html
name = unicode(name, "utf-8") # name.decode('utf-8')
print type(name)

print name[1:]
time = int(343.43)
print str.format("{}小时，{}分", time / 3600, time % 3600 / 60)
# hobby = u'这四个测试'
# hobby = hobby.encode('gb2312')
# hobby = hobby.decode('gb2312')
# print hobby







# db = MongoClient().aggregation_example
# db.things.insert({"x": 1, "tags": ["dog", "cat"]})
# db.things.insert({"x": 2, "tags": ["cat"]})
# db.things.insert({"x": 2, "tags": ["mouse", "cat", "dog"]})
# db.things.insert({"x": 3, "tags": []})
#
# temp = db.things.aggregate([
# {"$unwind": "$tags"},
# {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
# {"$sort": {'count':1}}
# ])
#
# while (temp.alive):
#     print temp.next()

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
