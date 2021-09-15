# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import numpy as np
import redis
import datetime
import logging
from lxml import etree
import requests
from requests.adapters import HTTPAdapter
from tools.update_user_feature import update_user_feature

class BangumiSpiderPipeline(object):
    def open_spider(self, spider):
        print('爬虫开始运行')
        self.client = pymongo.MongoClient('localhost')
        self.db = self.client['bangumi_test']
        self.anime_collection = self.db['anime1']
        self.collect_collection = self.db['collect']
        self.user_collection = self.db['user1']
        self.user_dropped_collection = self.db['user_dropped']
        self.tag_collection = self.db['tag']
        self.timeline_collection = self.db['timeline']
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.logger = logging.getLogger(__name__)
        # requests
        self.s = requests.session()
        self.s.keep_alive = False
        # 最大重试次数
        self.s.mount('http://', HTTPAdapter(max_retries=3))
        self.s.mount('https://', HTTPAdapter(max_retries=3))

    def process_item(self, item, spider):
        if item['item_type'] == 'anime': # 动画
            item.pop('item_type')
            self.anime_collection.insert(item)
        elif item['item_type'] == 'collect': # 保存动画每个观看状态的用户
            if self.collect_collection.find_one_and_update({'id': item['id'], 'state': item['state']}, {'$addToSet': {'users': {'$each': item['users']}}}) == None:
                self.collect_collection.insert_one({'id': item['id'], 'state': item['state'], 'users': item['users']})
        elif item['item_type'] == 'user': # 用户数据
            if self.user_collection.find_one_and_update({'user_id': item['user_id']}, {'$addToSet': {'collects': {'$each': item['collects']}}}) == None:
                item.pop('item_type')
                self.user_collection.insert_one(item)
        elif item['item_type'] == 'user_dropped':  # 用户数据
            if self.user_dropped_collection.find_one_and_update({'user_id': item['user_id']}, {
                '$addToSet': {'droppeds': {'$each': item['droppeds']}}}) == None:
                item.pop('item_type')
                self.user_dropped_collection.insert_one(item)
        elif item['item_type'] == 'tag': # 标签
            item.pop('item_type')
            self.tag_collection.insert(item)
        elif item['item_type'] == 'timeline':
            item.pop('item_type')
            self.timeline_collection.insert(item)
            if item['type'] == 2 and item['status'] == 'collect':
                self.logger.info('用户'+ str(item['user_id'])+'收藏动画'+str(item['subject_id']))
                # 更新mongo中用户收藏的动画
                if self.user_collection.find_one_and_update(
                    {
                        'user_id': int(item['user_id'])
                    },
                    {
                        '$addToSet': {'collects': {'$each': [[int(item['subject_id']), item['score'], item['timestamp'].strftime('%Y-%m-%d')]]}}
                    }) == None: # 如果MongoDB中没有此用户
                    self.logger.info('MongoDB新增用户：'+str(item['user_id']))
                    self.user_collection.insert_one({'user_id': item['user_id'], 'api': item['api'], 'collects': [[int(item['subject_id']), item['score'], item['timestamp'].strftime('%Y-%m-%d')]]})
                else: # MongoDB中有此用户
                    self.logger.info('MongoDB用户'+str(item['user_id'])+'新增收藏动画'+str(item['subject_id']))
                # 更新用户Embedding
                self.update_user_emb_by_id(item['user_id'])
                # 更新用户标签
                self.update_user_tag_by_id(item['user_id'])
                # 更新用户特征
                update_user_feature(item['user_id'])
                # 删除缓存
                self.r.delete("userRec:" + str(item['user_id']))

        return item

    def update_user_tag_by_id(self, user_id):
        """
        更新用户的标签
        :param user_id:
        :return:
        """
        res = self.s.get('https://bgm.tv/anime/list/{}/collect'.format(user_id), headers={
            'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'})
        res.encoding = 'utf-8'
        html = etree.HTML(res.text)
        tag_name_list = [str(name.title()) for name in html.xpath('//ul[@id="userTagList"]/li/a/text()')]
        tag_count_list = [int(count.title()) for count in html.xpath('//ul[@id="userTagList"]/li/a/small/text()')]
        tags = []
        for tag_name, tag_count in zip(tag_name_list, tag_count_list):
            tags.append({'name': tag_name, 'count': tag_count})
        # self.logger.info('MongoDB更新用户'+str(user_id)+'标签:'+str(tags))
        self.user_collection.update_one({'user_id': int(user_id)}, {'$set': {'tags': tags}})


    def update_user_emb_by_id(self, user_id):
        """
        更新Redis中的用户Embedding
        :param user_id:
        :return:
        """
        # 获取当前用户的所有收藏动画
        user = self.user_collection.find_one({'user_id': int(user_id)})
        # 取出当前用户收藏的所以动画id
        anime_ids = [i[0] for i in user['collects']]
        # 从redis 查询动画的Embdding
        anime_embs = np.array([self.get_emb_from_redis('animeEmb:' + str(anime_id)) for anime_id in anime_ids])
        # 过滤掉没有embedding的动画
        mask = [i is not None for i in anime_embs]
        anime_embs = anime_embs[mask]
        collects = np.array(user['collects'])[mask]
        # 计算每个动画的权重
        weights = [self.calc_weight(i) for i in collects]
        # 计算加权平均，得到用户的Embedding
        user_emb = None
        if len(anime_embs) > 0:
            # self.logger.info('重新计算用户' + str(user_id) + 'Embedding...')
            user_emb = np.average(anime_embs.tolist(), weights=weights, axis=0)
            # 更新redis中的用户Embedding
            emb = ','.join([str(i) for i in user_emb])
            self.r.set('userEmb:' + str(user_id), emb)
            self.logger.info('Redis更新用户' + str(user_id) + 'Embedding:' + str(emb))
        return user_emb

    def get_emb_from_redis(self, key):
        """
        从redis查询embedding字符串，并转换成list
        :param key:
        :return:
        """
        emb = self.r.get(key)
        if emb is not None:
            return [float(i) for i in emb.split(',')]
        return None

    def calc_weight(self, collect):
        """
        计算一条收藏记录的权重
        :param collect:
        :return:
        """
        # 时间衰减系数
        alpha1 = 0.0001
        # 评分系数
        alpha2 = 0.1
        event_time = datetime.datetime.strptime(collect[2], '%Y-%m-%d')
        # 时间权重
        weight1 = 1 / (1 + alpha1 * (datetime.datetime.now() - event_time).days)
        # 评分权重
        score = int(collect[1])
        weight2 = 1 if score == 0 else 1 / (1 + alpha2 * (5 - score))
        return weight1 * weight2

    def close_spider(self, spider):
        self.client.close()
