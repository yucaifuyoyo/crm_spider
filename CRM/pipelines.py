# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import time
from datetime import datetime


class CrmPipeline(object):
    def open_spider(self, spider):
        self.conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="root",
            database="crm_data",
            charset='utf8', )
        self.cursor = self.conn.cursor()
        pass

    def process_item(self, item, spider):
        pass
