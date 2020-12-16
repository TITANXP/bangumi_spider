import scrapy

class UserSpider(scrapy.Spider):
    name = 'user'
    allowed_domains = ['bgm.tv']
    base_url = 'http://bgm.tv'
    # start_usrls解析完成后，会传入parse()方法
    url_pattern = base_url + '/anime/list/{}/collect'
    start_urls = []
    for i in range(1, 600000):
        start_urls.append(url_pattern.format(i))

    def parse(self, response):
        if response.xpath('//div[@id="main"]/div[@class="columns clearit"]/div/div/p[@class="text"]/text()').get() == '数据库中没有查询到该用户的信息':
            print('用户',response.url.split('/')[-2],'不存在')
        else:
            id = response.url.split('/')[-2] if not 'redirect_urls' in response.request.meta else response.request.meta['redirect_urls'][0].split('/')[-2]
            # 爬取当前用户的信息
            request = scrapy.Request(response.url, self.parse_page)
            request.meta['id'] = id
            yield request
            # 爬取下一个用户
            # url = response.request.meta['redirect_urls'] # 获取重定向之前的url（有些用户页面在访问时会将id替换为用户名）
            # next_user = re.sub(r'(\d+)', lambda matched: str(int(matched.group(1))+1), url)
            # yield scrapy.Request(next_user, callback=self.parse)


    def parse_page(self, response):
        user = dict()
        user['item_type'] = 'user'
        user['id'] = response.request.meta['id']
        print(user['id'])
        user['items'] = []
        for item in response.xpath('//*[@id="browserItemList"]/li'):
            # 动画id
            item_id = int(item.xpath('./@id').get().replace('item_', ''))
            # 收藏时间
            timestamp =  item.xpath('./div/p[@class="collectInfo"]/span[@class="tip_j"]/text()').get()
            user['items'].append((item_id, timestamp))
        yield user
        # 爬取下一页
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            request = scrapy.Request(self.base_url + next_page, callback=self.parse_page)
            request.meta['id'] = user['id']
            yield request
