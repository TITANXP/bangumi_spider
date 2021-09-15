# -*- coding: utf-8 -*-
import scrapy
from scrapy import Item, Field
import datetime
import re
import json

# 爬取动画
class AnimeSpider(scrapy.Spider):
    name = 'anime'
    allowed_domains = ['bgm.tv']
    base_url = 'http://bgm.tv'
    # start_usrls解析完成后，会传入parse()方法
    start_urls = [base_url+'/anime/browser/']
    api_url = 'https://api.bgm.tv/subject/{}?responseGroup=large'

    # 解析动画列表
    def parse(self, response):
        # print(response.body.decode())
        # 将当前页的动画传入 parse_anime()进行处理
        for anime in response.xpath('//ul[@id="browserItemList"]/li/div//a/@href'):
            anime_url = self.base_url + anime.get()
            yield scrapy.Request(anime_url, callback=self.parse_anime)
        # 爬取下一页
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            next_page_url = self.base_url+ '/anime/browser/' + next_page
            yield scrapy.Request(next_page_url, callback=self.parse)

    # 解析动画页面
    def parse_anime(self, response):
        # anime = Item()
        anime = dict()
        anime['item_type'] = 'anime'
        anime['id'] = int(response.url.split('/')[-1])
        anime['create_time'] = datetime.datetime.now()
        # 如果页面出错之间返回
        if response.xpath('//div[@class="message"]/h2/text()').get() == "呜咕，出错了":
            print(response.url + " " + response.xpath('//div[@class="message"]/p[@class="text"]/text()').get())
            return anime
        # 爬取动画标题
        anime['name'] = response.xpath('//h1[@class="nameSingle"]/a/text()').get()
        # TV、OVA、剧场版
        anime['subtype'] = response.xpath('//h1[@class="nameSingle"]/small/text()').get()
        # 处理左侧的动画信息栏
        info_selector_list = response.xpath('//div[@id="bangumiInfo"]/div[@class="infobox"]/ul[@id="infobox"]/li')
        info = []
        for info_selector in info_selector_list:
            key = info_selector.xpath('./span/text()').get()
            value = info_selector.xpath('string(.)').get().strip().replace(key, '')
            key = key.replace(': ', '')
            # anime.fields[key] = Field()
            info.append([key, value])
        anime['info'] = info
        # 处理标签
        tag_selector_list = response.xpath('//div[@class="subject_tag_section"]//a')
        tag_list = []
        for tag_selector in tag_selector_list:
            tag_name = tag_selector.xpath('./span/text()').get()
            tag_count = int(tag_selector.xpath('./small/text()').get())
            tag_list.append({'name': tag_name, 'count': tag_count})
        anime['tags'] = tag_list
        # 简介
        anime['summary'] = response.xpath('//div[@id="subject_summary"]').xpath('string(.)').get()
        # 处理右侧收藏盒
        anime['rank'] = str(response.xpath('//div[@class="global_score"]/div/small[@class="alarm"]/text()').get()).replace('#', '')
        rating = {}
        #  评分
        rating['score'] = float(response.xpath('//div[@class="global_score"]/span[@class="number"]/text()').get())
        #  投票人数
        rating['total'] = int (response.xpath('//div[@id="ChartWarpper"]/div[@class="chart_desc"]//span/text()').get())
        #  每个评分对应的人数
        count = {}
        rating_count_item = [i.get() for i in response.xpath('//div[@id="ChartWarpper"]/ul/li/a/@title')][::-1]
        for (k, v) in zip(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], rating_count_item):
            count[k] = v
        rating['count'] = count
        anime['rating'] = rating
        # 不同观看状态对应的人数（人数为0时不会显示，取出来的为None，所以做了判断）
        collection = {}
        wishes = response.xpath('//div[@id="subjectPanelCollect"]/span/a[1]/text()').get()
        collection['wish'] = int (wishes.split("人")[0]) if wishes else 0
        collections = response.xpath('//div[@id="subjectPanelCollect"]/span/a[2]/text()').get()
        collection['collect'] = int (collections.split("人")[0]) if collections else 0
        doings = response.xpath('//div[@id="subjectPanelCollect"]/span/a[3]/text()').get()
        collection['doing'] = int (doings.split("人")[0]) if doings else 0
        on_hold = response.xpath('//div[@id="subjectPanelCollect"]/span/a[4]/text()').get()
        collection['on_hold'] = int (on_hold.split("人")[0]) if on_hold else 0
        dropped = response.xpath('//div[@id="subjectPanelCollect"]/span/a[5]/text()').get()
        collection['dropped'] = int (dropped.split("人")[0]) if dropped else 0
        anime['collection'] = collection
        # 请求角色页面，并将当前的anime对象传入，继续拼接属性
        request = scrapy.Request(response.url + '/characters', callback=self.parse_characters)
        request.meta['item'] = anime
        yield request
        # 爬取每个观看状态的用户信息
        collect_state_list = ['wishes', 'collections', 'doings', 'on_hold', 'dropped']
        # TODO: 关闭了收藏用户的爬取
        # for collect_state in collect_state_list:
        #     yield scrapy.Request(response.url + '/' + collect_state, callback=self.parse_collect)

    # 解析角色
    def parse_characters(self, response):
        anime = response.meta['item']
        character_selector_list = response.xpath('//div[@id="columnInSubjectA"]/div[@class="light_odd"]')
        character_list = []
        for character_selector in character_selector_list:
            character = {}
            # 不能根据角色左边的图片的超链接爬取，因为有的角色没有图片
            character['id'] = int (character_selector.xpath('./div[@class="clearit"]/h2/a/@href').get().split('/')[-1])
            character['name'] = character_selector.xpath('./div[@class="clearit"]/h2/a/text()').get()
            name_cn = character_selector.xpath('./div[@class="clearit"]/h2/span/text()').get()
            if name_cn:
                character['name_cn'] = name_cn[3:] # 去掉开始的'/'
            cv = character_selector.xpath('./div[@class="clearit"]/div[@class="actorBadge clearit"]/p/a/@href').get()
            if cv is not None:
                character['cv'] = int (cv.split('/')[-1])
            character_list.append(character)
        anime['character'] = character_list
        # 请求吐槽页面，并将当前的anime对象传入，继续拼接属性
        request = scrapy.Request(self.base_url + '/subject/' + str(anime['id']) + '/comments', callback=self.parse_comments)
        request.meta['item'] = anime
        yield request

    # 解析吐槽
    def parse_comments(self, response):
        anime = response.meta['item']
        comment_selector_list = response.xpath('//div[@id="comment_box"]/div')
        comment_list = []
        for comment_selector in comment_selector_list:
            comment = {}
            # 吐槽内容
            comment['summary'] = comment_selector.xpath('./div/div/p/text()').get()
            # 用户评分
            score = comment_selector.xpath('./div/div/span/span/@class').get()
            comment['score'] = int(score.replace('starlight stars', '')) if score else 0
            # 时间戳
            time = comment_selector.xpath('./div/div/small/text()').get().replace('@ ', '')
            if time.endswith('ago'):
                comment['create_time'] = self.get_datetime(time)
            else:
                comment['create_time'] = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M')
            # 用户信息
            user = {}
            user['id'] = comment_selector.xpath('./a/@href').get().split('/')[-1]
            user['nickname'] = comment_selector.xpath('./div/div/a/text()').get()
            user['url'] = 'https://bangumi.tv/user/' + user['id']
            user_avatar = {}
            user_avatar['small'] = 'http:'+comment_selector.xpath('./a/span/@style').get().replace("background-image:url('", "").replace("')", "")
            user_avatar['medium'] = user_avatar['small'].replace('/s/','/m/')
            user_avatar['large'] = user_avatar['small'].replace('/s/','/l/')
            user['avatar'] = user_avatar
            comment['user'] = user
            comment_list.append(comment)
        anime['comment'] = comment_list

        # 爬取下一页
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            next_page_url = self.base_url + '/subject/' + str(anime['id']) + '/comments' + next_page
            next_page_request = scrapy.Request(next_page_url, callback=self.parse_comments)
            next_page_request.meta['item'] = anime
            yield next_page_request
        # 如果没有下一页，则继续爬取其他内容
        else:
            # 请求网站API，并将当前的anime对象传入，继续拼接属性
            request = scrapy.Request(self.api_url.format(anime['id']), callback=self.parse_api)
            request.meta['item'] = anime
            yield request


    # 直接请求网站API得到的数据
    def parse_api(self, response):
        anime = response.meta['item']
        anime['api'] = json.loads(response.text)
        yield anime

    # 解析每个观看状态的用户信息
    def parse_collect(self, response):
        collect_item = {}
        collect_item['item_type'] = 'collect'
        # 作品id
        collect_item['id'] = int(re.match('.*subject/(\d*)', response.url)[1]) # [0]对应的是匹配到的整个字符串，[1]对应第一个（）中匹配到的内容
        # 'wishes', 'collections', 'doings', 'on_hold', 'dropped'
        collect_item['state'] = response.url.split('/')[-1].split('?')[0]

        # 爬取每个用户的信息
        user_div_list = response.xpath('//ul[@id="memberUserList"]/li/div')
        user_list = []
        #   用户id，收藏时间
        for user_div in user_div_list:
            # TODO: 如果用户设置了用户名，则通过个人主页链接爬取的是用户名，而不是数字id
            #       如果通过头像连接爬取，则没有设置头像的用户不能爬取。
            #       暂时根据个人主页连接爬取
            user_id = user_div.xpath('./strong/a/@href').get().split('/')[-1]
            collect_time = datetime.datetime.strptime(user_div.xpath('./p/text()').get(), '%Y-%m-%d %H:%M')
            user_list.append((user_id, collect_time))
        collect_item['users'] = user_list
        # 爬取下一页用户
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            yield scrapy.Request(self.base_url + next_page, callback=self.parse_collect)
        yield collect_item

    # 将'7h 48m  ago'格式的字符串转换为日期
    def get_datetime(self, str):
        time_units = str.replace('  ago', '').split()
        days, hours, minutes, seconds = 0, 0, 0, 0
        for time_unit in time_units:
            if time_unit.endswith('d'):
                days = int(time_unit[:-1])
            elif time_unit.endswith('h'):
                hours = int(time_unit[:-1])
            elif time_unit.endswith('m'):
                minutes = int(time_unit[:-1])
            elif time_unit.endswith('s'):
                seconds = int(time_unit[:-1])
        time_delta =  datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        return (datetime.datetime.now() - time_delta)


















