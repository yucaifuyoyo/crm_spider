# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class EastpharmItem(scrapy.Item):
    """华东医药"""
    company_id = scrapy.Field()
    delivery_id = scrapy.Field()
    data_version = scrapy.Field()
    data_type = scrapy.Field() # python　excel

    bill_type = scrapy.Field()
    sell_time = scrapy.Field()
    drug_name = scrapy.Field()
    drug_specification = scrapy.Field()
    supplier_name = scrapy.Field()
    delivery_name = scrapy.Field()
    hospital_name = scrapy.Field()
    drug_batch = scrapy.Field()
    valid_till = scrapy.Field() # 有效期
    drug_number = scrapy.Field()
    drug_price = scrapy.Field()
    hospital_address = scrapy.Field()
    create_time = scrapy.Field()

    drug_hash = scrapy.Field()
    drug_unit = scrapy.Field()
    hospital_hash = scrapy.Field()


class EastpharmCrmItem(scrapy.Item):
    """华东医药"""
    company_id = scrapy.Field()
    delivery_id = scrapy.Field()
    foreign_id = scrapy.Field()
    data_version = scrapy.Field()
    data_type = scrapy.Field() # python　excel

    bill_type = scrapy.Field()
    sell_time = scrapy.Field()
    drug_name = scrapy.Field()
    drug_specification = scrapy.Field()
    supplier_name = scrapy.Field()
    delivery_name = scrapy.Field()
    hospital_name = scrapy.Field()
    drug_batch = scrapy.Field()
    valid_till = scrapy.Field() # 有效期
    drug_number = scrapy.Field()
    drug_price = scrapy.Field()
    hospital_address = scrapy.Field()

    drug_hash = scrapy.Field()
    drug_unit = scrapy.Field()
    hospital_hash = scrapy.Field()


class VerItem(scrapy.Item):
    verId = scrapy.Field()