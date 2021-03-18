# -*- coding: utf-8 -*-
import scrapy


class TagSpider(scrapy.Spider):
    name = 'tag'
    allowed_domains = ['bgm.tv']
    base_url = 'http://bgm.tv'
    start_urls = [base_url+'/anime/tag']

    def parse(self, response):

        tag_selector_list = response.xpath('//div[@id="tagList"]/a')
        count_selector_list = response.xpath('//div[@id="tagList"]/small')
        for tag_selector,count_selector in zip(tag_selector_list, count_selector_list):
            tag = dict()
            tag['item_type'] = 'tag'
            tag['name'] = tag_selector.xpath('./text()').get()
            tag['count'] = int(count_selector.xpath('./text()').get()[1:-1])
            yield tag
        # 爬取下一页
        next_page = response.xpath('//div[@class="page_inner"]/a[contains(text(), "››")]/@href').get()
        if next_page:
            next_page_url = self.base_url + '/anime/tag' + next_page
            yield scrapy.Request(next_page_url, callback=self.parse)

