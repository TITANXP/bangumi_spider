import scrapy
import re
import datetime
import time
import requests
import pymongo

class UserSpider(scrapy.Spider):
    name = 'timeline'
    allowed_domains = ['bgm.tv']
    base_url = 'http://bgm.tv'
    api_base_url = 'https://api.bgm.tv'
    # start_usrls解析完成后，会传入parse()方法
    start_urls = ['https://bgm.tv/timeline?type=subject']
    # 切分时间的正则
    time_unit_pattern = re.compile(r'\d+\D') # \d:任意数字  \D：任意非数字
    # 收藏状态的正则
    wish_pattern = re.compile(r'想\S') # 想看 想读 想玩
    collect_pattern = re.compile(r'\S过')
    doing_pattern = re.compile(r'在\S')
    # mongo
    client = pymongo.MongoClient('localhost')
    db = client['bangumi_test']
    timeline_collection = db['timeline']
    # requests
    s = requests.session()
    s.keep_alive = False

    def parse(self, response):

        logs = []
        now = datetime.datetime.now()
        # 遍历当前页面的每个条目
        for item_selector in response.xpath('//div[@id="timeline"]/ul/li'):
            # 一个用户的收藏
            ids = [href.get().split('/')[-1] for href in item_selector.xpath('./span[@class="info clearit"]/a/@href')]
            # 如果一个item下面只有一个动画，第一个<a>是动画的url
            if item_selector.xpath('./span[@class="info clearit"]/a/@href')[0].get().split('/')[-2] != 'user':
                ids = ids[1:]
            # 获取用户id
            username = ids[0]
            user_api_data = self.s.get(self.api_base_url+ '/user/' + username,headers={
                'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'}).json()
            user_id = user_api_data['id']
            # 事件事件
            timestamp = self.get_datetime(item_selector.xpath('./span[@class="info clearit"]/p/text()').get(), now)
            # 评分
            score = item_selector.xpath('./span[@class="info clearit"]/div[@class="collectInfo"]/span[@class="starstop-s"]/span/@class').get()
            score = int(score.replace('starlight stars', '')) if score else 0 # 未评分则设置为0
            # 收藏状态
            status = self.get_status(item_selector.xpath('./span[@class="info clearit"]/text()')[0].get())
            for subject_id in ids[1:]:
                log = {'item_type': 'timeline', 'user_id': user_id, 'subject_id': subject_id, 'status': status, 'score': score, 'timestamp': timestamp, 'api': user_api_data}
                # 判断此记录是否已存在
                is_exists = self.timeline_collection.find_one(
                    {'user_id': log['user_id'], 'subject_id': log['subject_id'], 'status': log['status'], 'score': log['score'],
                            'timestamp': {'$gt': log['timestamp'] - datetime.timedelta(minutes=5)}
                     })
                if not is_exists:
                    # logs.append(log)
                    log['type'] = self.s.get(self.api_base_url+ '/subject/' + log['subject_id'] +'?responseGroup=small',headers={
                        'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'}).json()['type']
                    print(log)
                    yield log
        time.sleep(7)
        yield scrapy.Request('https://bgm.tv/timeline?type=subject', callback=self.parse, dont_filter=True) # dont_filter=True不过滤之前爬取过的url

    def get_datetime(self, str, time):
        """
         将'52秒前 · web', '1分11秒前 · web'格式的字符串转换为日期
        :param str:
        :param time: 事件时间
        :return:
        """
        time_units = self.time_unit_pattern.findall(str.split('前')[0])
        days, hours, minutes, seconds = 0, 0, 0, 0
        for time_unit in time_units:
            if time_unit.endswith('天'):
                days = int(time_unit[:-1])
            elif time_unit.endswith('时'):
                hours = int(time_unit[:-1])
            elif time_unit.endswith('分'):
                minutes = int(time_unit[:-1])
            elif time_unit.endswith('秒'):
                seconds = int(time_unit[:-1])
        time_delta =  datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        return (time - time_delta)

    # 根据字符串解析收藏状态
    def get_status(self, str):
        str = str.strip()
        if self.wish_pattern.search(str):
            return 'wish'
        elif self.collect_pattern.search(str):
            return 'collect'
        elif self.doing_pattern.search(str):
            return 'doing'
        elif str == '搁置了':
            return 'on_hold'
        elif str == '抛弃了':
            return 'dropped'
        else:
            return '未知：'+str




