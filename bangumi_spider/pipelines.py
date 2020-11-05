# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

class BangumiSpiderPipeline(object):
    def open_spider(self, spider):
        print('爬虫开始运行')
        self.client = pymongo.MongoClient('localhost', )
        self.db = self.client['bangumi_test']
        self.anime_collection = self.db['anime']
        self.collect_collection = self.db['collect']
        self.user_collection = self.db['user']

    def process_item(self, item, spider):
        if item['item_type'] == 'anime': # 动画
            item.pop('item_type')
            self.anime_collection.insert(item)
        elif item['item_type'] == 'collect': # 保存动画每个观看状态的用户
            if self.collect_collection.find_one_and_update({'id': item['id'], 'state': item['state']}, {'$addToSet': {'users': {'$each': item['users']}}}) == None:
                self.collect_collection.insert_one({'id': item['id'], 'state': item['state'], 'users': item['users']})
        elif item['item_type'] == 'user': # 用户数据
            if self.user_collection.find_one_and_update({'id': item['id']}, {'$addToSet': {'items': {'$each': item['items']}}}) == None:
                self.user_collection.insert_one({'id': item['id'], 'items': item['items']})
        return item

    def close_spider(self, spider):
        self.client.close()
