import scrapy
import json


class UserSpider(scrapy.Spider):
    name = 'user_dropped'
    allowed_domains = ['bgm.tv']
    # base_url = 'http://bgm.tv'
    base_url = 'https://bgm.tv'
    api_base_url = 'https://api.bgm.tv'
    # start_usrls解析完成后，会传入parse()方法
    url_pattern = base_url + '/anime/list/{}/dropped'
    api_url_pattern = api_base_url + '/user/{}'
    start_urls = [api_base_url + '/user/126500']
    # for i in range(1, 600000):
    #     start_urls.append(url_pattern.format(i))

    def parse(self, response):
        # if response.xpath('//div[@id="main"]/div[@class="columns clearit"]/div/div/p[@class="text"]/text()').get() == '数据库中没有查询到该用户的信息':
        user_id = int(response.url.split('/')[-1])
        # 请求api获取的数据
        api_data = json.loads(response.text)
        if 'code' in api_data and api_data['code'] == 404:
            print('用户',user_id,'不存在')
        else:
            user = dict()
            user['item_type'] = 'user_dropped'
            user['api'] = api_data
            user['user_id'] = api_data['id']
            # user_id = response.url.split('/')[-2] if not 'redirect_urls' in response.request.meta else response.request.meta['redirect_urls'][0].split('/')[-2]
            # 爬取当前用户的信息
            request = scrapy.Request(self.url_pattern.format(user['user_id']), self.parse_page)
            request.meta['item'] = user
            yield request
        # 爬取下一个用户
        if user_id < 603542:
            yield scrapy.Request(self.api_url_pattern.format(user_id+1), callback=self.parse)


    def parse_page(self, response):
        user = response.meta['item']
        # 爬取看过的动画
        user['droppeds'] = []
        for item in response.xpath('//*[@id="browserItemList"]/li'):
            # 动画id
            item_id = int(item.xpath('./@id').get().replace('item_', ''))
            # 评分
            score = item.xpath('./div/p[@class="collectInfo"]/span[@class="starstop-s"]/span/@class').get()
            score = int(score.replace('starlight stars', '')) if score else 0 # 未评分则设置为0
            # 收藏时间
            timestamp =  item.xpath('./div/p[@class="collectInfo"]/span[@class="tip_j"]/text()').get()
            user['droppeds'].append((item_id, score, timestamp))
        # 如果爬取的是第一页，则爬取用户标签
        if 'api' in user:
            tag_selector_list = response.xpath('//ul[@id="userTagList"]/li/a')
            count_selector_list = response.xpath('//ul[@id="userTagList"]/li/a/small')
            tags = []
            for tag_selector, count_selector in zip(tag_selector_list, count_selector_list):
                tag = dict()
                tag['name'] = tag_selector.xpath('./text()').get()
                tag['count'] = int(count_selector.xpath('./text()').get())
                tags.append(tag)
            user['tags'] = tags
        yield user # 爬完一页后插入数据库，爬取下一页后更新数据
        # 爬取下一页看过的动画
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            request = scrapy.Request(self.base_url + next_page, callback=self.parse_page)
            request.meta['item'] = {'user_id': user['user_id'], 'item_type': 'user_dropped'}
            yield request
