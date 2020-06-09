# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
from pymongo import MongoClient
from kafka import KafkaProducer

mongo = MongoClient(
    host=os.environ.get('CRAWLAB_MONGO_HOST') or 'localhost',
    port=int(os.environ.get('CRAWLAB_MONGO_PORT') or 27017),
    username=os.environ.get('CRAWLAB_MONGO_USERNAME'),
    password=os.environ.get('CRAWLAB_MONGO_PASSWORD'),
    authSource=os.environ.get('CRAWLAB_MONGO_AUTHSOURCE') or 'admin'
)
db = mongo[os.environ.get('CRAWLAB_MONGO_DB') or 'test']
col = db[os.environ.get('CRAWLAB_COLLECTION') or 'test']
kafka_nodes = os.environ.get('KAFKA_NODES')

topic = os.environ.get('CRAWLAB_COLLECTION')
task_id = os.environ.get('CRAWLAB_TASK_ID')

class ConfigSpiderPipeline(object):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=kafka_nodes.split(','))

    def process_item(self, item, spider):
        item['task_id'] = task_id
        col.save(item)
        self.producer.send(topic, str(item).encode(encoding='utf_8'))
        return item

    def spider_closed(self, spider):
        self.producer.close()