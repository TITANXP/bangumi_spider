# -*- coding: utf-8 -*-
import scrapy
from scrapy import Item, Field
import datetime
import re

# 爬取动画
class AnimeSpider(scrapy.Spider):
    name = 'anime'
    allowed_domains = ['bangumi.tv']
    # start_usrls解析完成后，会传入parse()方法
    start_urls = ['https://bangumi.tv/anime/browser/']

    # 处理动画列表
    def parse(self, response):
        # print(response.body.decode())
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        # 将当前页的动画传入 parse_anime()进行处理
        for anime in response.xpath('//ul[@id="browserItemList"]/li/div//a/@href'):
            anime_url = 'https://bangumi.tv' + anime.get()
            yield scrapy.Request(anime_url, callback=self.parse_anime)
        # 爬取下一页
        if next_page:
            next_page_url = 'https://bangumi.tv/anime/browser/' + next_page
            yield scrapy.Request(next_page_url, callback=self.parse)

    # 处理动画页面
    def parse_anime(self, response):
        # anime = Item()
        anime = dict()
        anime['item_type'] = 'anime'
        anime['id'] = int(response.url.split('/')[-1])
        anime['create_time'] = datetime.datetime.now()
        # 如果页面出错之间返回A
        if response.xpath('//div[@class="message"]/h2/text()').get() == "呜咕，出错了":
            print(response.url + " " + response.xpath('//div[@class="message"]/p[@class="text"]/text()').get())
            return anime
        # 爬取动画标题
        anime['name'] = response.xpath('//h1[@class="nameSingle"]/a/text()').get()
        # TV、OVA、剧场版
        anime['type'] = response.xpath('//h1[@class="nameSingle"]/small/text()').get()
        # 处理左侧的动画信息栏
        info_box = response.xpath('//div[@id="bangumiInfo"]/div[@class="infobox"]/ul[@id="infobox"]/li')
        for info_item in info_box:
            key = info_item.xpath('./span/text()').get()
            value = info_item.xpath('string(.)').get().strip().replace(key, '')
            key = key.replace(': ', '')
            # anime.fields[key] = Field()
            anime[key] = value
        # 处理标签
        tag_section = response.xpath('//div[@class="subject_tag_section"]//a')
        tag_list = []
        for tag in tag_section:
            tag_name = tag.xpath('./span/text()').get()
            tag_sum = int(tag.xpath('./small/text()').get())
            tag_list.append((tag_name, tag_sum))
        anime['tags'] = tag_list
        # 简介
        anime['summary'] =  response.xpath('//div[@id="subject_summary"]').xpath('string(.)').get()
        # 评分
        anime['score'] = float(response.xpath('//div[@class="global_score"]/span[@class="number"]/text()').get())
        # 投票人数
        anime['votes'] = int (response.xpath('//div[@id="ChartWarpper"]/div[@class="chart_desc"]//span/text()').get())
        # 不同观看情况对应的人数（人数为0会不显示，取出来的为None）
        wishes = response.xpath('//div[@id="subjectPanelCollect"]/span/a[1]/text()').get()
        anime['wishes'] = int (wishes.split("人")[0]) if wishes else 0
        collections = response.xpath('//div[@id="subjectPanelCollect"]/span/a[2]/text()').get()
        anime['collections'] = int (collections.split("人")[0]) if collections else 0
        doings = response.xpath('//div[@id="subjectPanelCollect"]/span/a[3]/text()').get()
        anime['doings'] = int (doings.split("人")[0]) if doings else 0
        on_hold = response.xpath('//div[@id="subjectPanelCollect"]/span/a[4]/text()').get()
        anime['on_hold'] = int (on_hold.split("人")[0]) if on_hold else 0
        dropped = response.xpath('//div[@id="subjectPanelCollect"]/span/a[5]/text()').get()
        anime['dropped'] = int (dropped.split("人")[0]) if dropped else 0
        # 请求角色页面，并将当前的anime对象传入，继续拼接属性
        request = scrapy.Request(response.url + '/characters', callback=self.parse_characters)
        request.meta['item'] = anime
        yield request
        # 爬取每个观看状态的用户信息
        collect_state_list = ['wishes', 'collections', 'doings', 'on_hold', 'dropped']
        # TODO: 关闭了收藏用户的爬取
        # for collect_state in collect_state_list:
        #     yield scrapy.Request(response.url + '/' + collect_state, callback=self.parse_collect)

    # 处理角色
    def parse_characters(self, response):
        anime = response.meta['item']
        character_info_list = response.xpath('//div[@id="columnInSubjectA"]/div[@class="light_odd"]')
        character_list = []
        for character_info in character_info_list:
            character = {}
            # 不能根据角色左边的图片的超链接爬取，因为有的角色没有图片
            character['id'] = int (character_info.xpath('./div[@class="clearit"]/h2/a/@href').get().split('/')[-1])
            character['name'] = character_info.xpath('./div[@class="clearit"]/h2/a/text()').get()
            name_cn = character_info.xpath('./div[@class="clearit"]/h2/span/text()').get()
            if name_cn:
                character['name_cn'] = name_cn[3:] # 去掉开始的'/'
            cv = character_info.xpath('./div[@class="clearit"]/div[@class="actorBadge clearit"]/p/a/@href').get()
            if cv is not None:
                character['cv'] = int (cv.split('/')[-1])
            character_list.append(character)
        anime['characters'] = character_list
        yield anime

    # 处理每个观看状态的用户信息
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
            yield scrapy.Request('https://bangumi.tv/' + next_page, callback=self.parse_collect)
        yield collect_item













