# -*- coding: utf-8 -*-
import scrapy
import requests
from lxml import etree
from requests.utils import dict_from_cookiejar
import os
import urllib
import time
import re
from CRM.spiders.YDM import YDMHttp
import pandas as pd
import numpy as np
import xlrd
from xlrd import xldate_as_tuple
import uuid
import datetime
import hashlib
import pymysql
import datetime
import json
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE
import platform


class WenzhouyingteSpider(scrapy.Spider):
    name = 'wenzhouyingte_a'
    allowed_domains = ['.cn']
    start_urls = ['http://www.wzmed.cn/Login.aspx']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    # fist = '2018-07-06'
    fist = FIST
    # last = '2018-11-05'
    last = LAST
    sess_wenzhouyingte_a = requests.session()
    db = pymysql.connect(host=HOST, port=PORT, database=DATABASE,
                         user=USER, password=PASSWORD,
                         charset='utf8')
    cursor = db.cursor()

    crm_db = pymysql.connect(host=HOST_CRM, port=PORT_CRM, database=DATABASE_CRM,
                             user=USER_CRM, password=PASSWORD_CRM,
                             charset='utf8')
    crm_cursor = crm_db.cursor()
    number = 0
    classify_success = 'false'
    gvGoods = {}

    def parse(self, response):
        delivery_id = 'F617B115D6F3447983E94BB781231233'
        self.crm_cursor.execute(
            "select company_id, enterprise_name, get_account, get_pwd, is_enable from base_delivery_enterprise where delivery_id = '{}'".format(
                delivery_id))

        data_tupl = self.crm_cursor.fetchall()
        for data_info in data_tupl:
            company_id = data_info[0]
            enterprise_name = data_info[1]
            get_account = data_info[2]
            get_pwd = data_info[3]
            is_enable = data_info[4]

        if is_enable == 1:
            self.number += 1
            login_html = self.sess_wenzhouyingte_a.get(url=self.start_urls[0], headers=self.headers, verify=False)
            login_resp = etree.HTML(login_html.content.decode('utf-8'))
            # print("login_html.content.decode('utf-8')", login_html.content.decode('utf-8'))
            __VIEWSTATE = login_resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __VIEWSTATEGENERATOR = login_resp.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value')[0]
            __EVENTVALIDATION = login_resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]

            login_data = {
                "__LASTFOCUS": "",
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "txtUserName": get_account,
                "txtPwd": get_pwd,
            }
            login_html = self.sess_wenzhouyingte_a.post(url='http://www.wzmed.cn/Login.aspx', data=login_data,
                                                        headers=self.headers,
                                                        verify=False)
            # print('login_html', login_html.content.decode('utf-8'))
            psot_data_html = self.sess_wenzhouyingte_a.get(url='http://www.wzmed.cn/Sales.aspx', headers=self.headers,
                                                           verify=False)
            # print('psot_data_html', psot_data_html.content.decode('utf-8'))
            # print('*' * 1000)
            try:
                data_resp = etree.HTML(psot_data_html.content.decode('utf-8'))
                gvGoods_name = data_resp.xpath('//*[@id="gvGoodsList"]/tr/td/input/@name')
                __VIEWSTATE = data_resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                __VIEWSTATEGENERATOR = data_resp.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value')[0]
                __EVENTVALIDATION = data_resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                data_data = {
                    "__EVENTTARGET": "btnSaleList",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": __VIEWSTATE,
                    "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                    "__EVENTVALIDATION": __EVENTVALIDATION,
                    "cbAll": "on",
                    "txtStartTime": self.fist,
                    "txtEndTime": self.last,
                    "txtCustom": "",
                    # "gvGoodsList$ctl02$CheckBox1": "on",
                    # "gvGoodsList$ctl03$CheckBox1": "on",
                    # "gvGoodsList$ctl04$CheckBox1": "on",
                    # "gvGoodsList$ctl05$CheckBox1": "on",
                    # "gvGoodsList$ctl06$CheckBox1": "on",
                    # "gvGoodsList$ctl07$CheckBox1": "on",
                    # "gvGoodsList$ctl08$CheckBox1": "on",
                    # "gvGoodsList$ctl09$CheckBox1": "on",
                    # "gvGoodsList$ctl10$CheckBox1": "on",
                    # "gvGoodsList$ctl11$CheckBox1": "on",
                    # "gvGoodsList$ctl12$CheckBox1": "on",
                    # "gvGoodsList$ctl13$CheckBox1": "on",
                    # "gvGoodsList$ctl14$CheckBox1": "on",
                    # "gvGoodsList$ctl15$CheckBox1": "on",
                    # "gvGoodsList$ctl16$CheckBox1": "on",
                    # "gvGoodsList$ctl17$CheckBox1": "on",
                    # "gvGoodsList$ctl18$CheckBox1": "on",
                    # "gvGoodsList$ctl19$CheckBox1": "on",
                    # "gvGoodsList$ctl20$CheckBox1": "on",
                    # "gvGoodsList$ctl21$CheckBox1": "on",
                    # "gvGoodsList$ctl22$CheckBox1": "on",
                    # "gvGoodsList$ctl23$CheckBox1": "on",
                    # "gvGoodsList$ctl24$CheckBox1": "on",
                    # "gvGoodsList$ctl25$CheckBox1": "on",
                    # "gvGoodsList$ctl26$CheckBox1": "on",
                    # "gvGoodsList$ctl27$CheckBox1": "on",
                    # "gvGoodsList$ctl28$CheckBox1": "on",
                    # "gvGoodsList$ctl29$CheckBox1": "on",
                    # "gvGoodsList$ctl30$CheckBox1": "on",
                    # "gvGoodsList$ctl31$CheckBox1": "on",
                    # "gvGoodsList$ctl32$CheckBox1": "on",
                    # "gvGoodsList$ctl33$CheckBox1": "on",
                    # "gvGoodsList$ctl34$CheckBox1": "on",
                    # "gvGoodsList$ctl35$CheckBox1": "on",
                    # "gvGoodsList$ctl36$CheckBox1": "on",
                    # "gvGoodsList$ctl37$CheckBox1": "on",
                    # "gvGoodsList$ctl38$CheckBox1": "on",
                    # "gvGoodsList$ctl39$CheckBox1": "on",
                    # "gvGoodsList$ctl40$CheckBox1": "on",
                    # "gvGoodsList$ctl41$CheckBox1": "on",
                    # "gvGoodsList$ctl42$CheckBox1": "on",
                    # "gvGoodsList$ctl43$CheckBox1": "on",
                }
                gvGoods_value = []
                for i in range(len(gvGoods_name)):
                    gvGoods_value.append('no')
                self.gvGoods = dict(zip(gvGoods_name, gvGoods_value))
                data_data = dict(data_data, **self.gvGoods)
                # print('data_data', data_data)
                self.sess_wenzhouyingte_a.post(url='http://www.wzmed.cn/Sales.aspx', data=data_data,
                                               headers=self.headers, verify=False)
                data_html = self.sess_wenzhouyingte_a.get(url='http://www.wzmed.cn/SaleList.aspx', headers=self.headers,
                                                          verify=False)
                # print('data_html', data_html.content.decode('utf-8'))
                data_html = etree.HTML(data_html.content.decode('utf-8'))
                data_len = int(len(data_html.xpath('//*[@id="gvSaleList"]/tr'))) - 1
                # print(data_len)
                md5 = hashlib.md5()
                for i in range(data_len):
                    # 入驻企业id
                    company_id = company_id
                    # 配送公司id
                    delivery_id = delivery_id
                    # 配送公司名称
                    delivery_name = enterprise_name
                    # 数据版本号
                    data_version = delivery_id + "-" + self.time_stamp
                    # 数据类型:1,phython 2,导入
                    data_type = 1
                    # 单据类型:1进货,2退货,3销售,4销售退货
                    bill_type = 3
                    try:
                        drug_name = data_html.xpath(
                            '//*[@id="gvSaleList"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[7]/text()' % (i + 2))[0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[5]/text()' % (i + 2))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = round(float(data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()))
                            if drug_number < 0:
                                bill_type = 4
                        # print('drug_number', drug_number)
                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[6]/text()' % (i + 2))[0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = '2000-01-01'
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            hospital_name = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[11]/text()' % (i + 2))[0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[13]/text()' % (i + 2))[0].strip()
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[1]/text()' % (i + 2))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_weizhouyingte'

                        try:
                            # 单价
                            drug_price = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[9]/text()' % (i + 2))[0].strip()
                        except:
                            drug_price = ''

                        try:
                            # 商品id
                            goods_id = data_html.xpath(
                                '//*[@id="gvSaleList"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                        except:
                            goods_id = ''

                        drug_hashs = "%s %s %s %s" % (drug_name, drug_specification, delivery_id, supplier_name)
                        md5 = hashlib.md5()
                        md5.update(bytes(drug_hashs, encoding="utf-8"))
                        drug_hash = md5.hexdigest()
                        hospital_hashs = "%s %s %s" % (delivery_id, hospital_name, hospital_address)
                        md5 = hashlib.md5()
                        md5.update(bytes(hospital_hashs, encoding="utf-8"))
                        hospital_hash = md5.hexdigest()
                        stream_hashs = "%s %s %s %s %s %s %s %s %s %s" % (
                            company_id, delivery_id, bill_type, drug_hash, drug_unit, abs(drug_number), drug_batch,
                            valid_till,
                            hospital_hash, sell_time)
                        md5 = hashlib.md5()
                        md5.update(bytes(stream_hashs, encoding="utf-8"))
                        stream_hash = md5.hexdigest()
                        month = int(str(self.fist).replace('-', '')[0: 6])

                        sql_crm = "insert into order_metadata_weizhouyingte(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, goods_id, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, sell_time, create_time, update_time, drug_price,
                                                      goods_id,
                                                      drug_hash, hospital_hash, stream_hash, month)
                        # print('sql_data', sql_data_crm)
                        try:
                            self.db.ping()
                        except pymysql.MySQLError:
                            self.db.connect()

                        try:
                            self.cursor.execute(sql_data_crm)
                            self.db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                        self.cursor.execute('select max(id) from order_metadata_weizhouyingte')
                        foreign_id = self.cursor.fetchone()[0]

                        sql_crm_data = SQL_CRM_DATA
                        sql_data_crm_data = sql_crm_data.format(company_id, delivery_id, delivery_name, table_name,
                                                                foreign_id,
                                                                data_version,
                                                                data_type, bill_type, drug_name, drug_specification,
                                                                supplier_name,
                                                                drug_hash, drug_unit, abs(drug_number), drug_batch,
                                                                valid_till,
                                                                hospital_name,
                                                                hospital_address, hospital_hash, month, sell_time,
                                                                stream_hash,
                                                                create_time, update_time)

                        try:
                            self.cursor.execute(sql_data_crm_data)
                            self.db.commit()
                            self.crm_cursor.execute(sql_data_crm_data)
                            self.crm_db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                psot_data_html_cai = self.sess_wenzhouyingte_a.get(url='http://www.wzmed.cn/Stock.aspx',
                                                                   headers=self.headers, verify=False)
                # print('psot_data_html_cai', psot_data_html_cai.content.decode('utf-8'))
                # print('*' * 1000)
                data_resp = etree.HTML(psot_data_html_cai.content.decode('utf-8'))
                __VIEWSTATE = data_resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                __VIEWSTATEGENERATOR = data_resp.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value')[0]
                __EVENTVALIDATION = data_resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                data_data = {
                    "__EVENTTARGET": "btnPurchaseList",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": __VIEWSTATE,
                    "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                    "__EVENTVALIDATION": __EVENTVALIDATION,
                    "cbAll": "on",
                    "txtStartTime": self.fist,
                    "txtEndTime": self.last,
                    # "gvGoodsList$ctl02$CheckBox1": "on",
                    # "gvGoodsList$ctl03$CheckBox1": "on",
                    # "gvGoodsList$ctl04$CheckBox1": "on",
                    # "gvGoodsList$ctl05$CheckBox1": "on",
                    # "gvGoodsList$ctl06$CheckBox1": "on",
                    # "gvGoodsList$ctl07$CheckBox1": "on",
                    # "gvGoodsList$ctl08$CheckBox1": "on",
                    # "gvGoodsList$ctl09$CheckBox1": "on",
                    # "gvGoodsList$ctl10$CheckBox1": "on",
                    # "gvGoodsList$ctl11$CheckBox1": "on",
                    # "gvGoodsList$ctl12$CheckBox1": "on",
                    # "gvGoodsList$ctl13$CheckBox1": "on",
                    # "gvGoodsList$ctl14$CheckBox1": "on",
                    # "gvGoodsList$ctl15$CheckBox1": "on",
                    # "gvGoodsList$ctl16$CheckBox1": "on",
                    # "gvGoodsList$ctl17$CheckBox1": "on",
                    # "gvGoodsList$ctl18$CheckBox1": "on",
                    # "gvGoodsList$ctl19$CheckBox1": "on",
                    # "gvGoodsList$ctl20$CheckBox1": "on",
                    # "gvGoodsList$ctl21$CheckBox1": "on",
                    # "gvGoodsList$ctl22$CheckBox1": "on",
                    # "gvGoodsList$ctl23$CheckBox1": "on",
                    # "gvGoodsList$ctl24$CheckBox1": "on",
                    # "gvGoodsList$ctl25$CheckBox1": "on",
                    # "gvGoodsList$ctl26$CheckBox1": "on",
                    # "gvGoodsList$ctl27$CheckBox1": "on",
                    # "gvGoodsList$ctl28$CheckBox1": "on",
                    # "gvGoodsList$ctl29$CheckBox1": "on",
                    # "gvGoodsList$ctl30$CheckBox1": "on",
                    # "gvGoodsList$ctl31$CheckBox1": "on",
                    # "gvGoodsList$ctl32$CheckBox1": "on",
                    # "gvGoodsList$ctl33$CheckBox1": "on",
                    # "gvGoodsList$ctl34$CheckBox1": "on",
                    # "gvGoodsList$ctl35$CheckBox1": "on",
                    # "gvGoodsList$ctl36$CheckBox1": "on",
                    # "gvGoodsList$ctl37$CheckBox1": "on",
                    # "gvGoodsList$ctl38$CheckBox1": "on",
                    # "gvGoodsList$ctl39$CheckBox1": "on",
                    # "gvGoodsList$ctl40$CheckBox1": "on",
                    # "gvGoodsList$ctl41$CheckBox1": "on",
                    # "gvGoodsList$ctl42$CheckBox1": "on",
                    # "gvGoodsList$ctl43$CheckBox1": "on",
                }
                data_data = dict(data_data, **self.gvGoods)
                self.sess_wenzhouyingte_a.post(url='http://www.wzmed.cn/Stock.aspx', data=data_data,
                                               headers=self.headers, verify=False)
                data_html_cai = self.sess_wenzhouyingte_a.get(url='http://www.wzmed.cn/PurchaseList.aspx',
                                                              headers=self.headers,
                                                              verify=False)
                # print('data_html', data_html_cai.content.decode('utf-8'))
                data_html_cai = etree.HTML(data_html_cai.content.decode('utf-8'))
                data_len = int(len(data_html_cai.xpath('//*[@id="gvPurchaseList"]/tr'))) - 1
                # print(data_len)
                md5 = hashlib.md5()
                for i in range(data_len):
                    # 入驻企业id
                    company_id = company_id
                    # 配送公司id
                    delivery_id = delivery_id
                    # 配送公司名称
                    delivery_name = enterprise_name
                    # 数据版本号
                    data_version = delivery_id + "-" + self.time_stamp
                    # 数据类型:1,phython 2,导入
                    data_type = 1
                    # 单据类型:1进货,2退货,3销售,4销售退货
                    bill_type = 1
                    try:
                        drug_name = data_html_cai.xpath(
                            '//*[@id="gvPurchaseList"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[7]/text()' % (i + 2))[0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[5]/text()' % (i + 2))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = round(float(data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()))
                            if drug_number < 0:
                                bill_type = 2
                        # print('drug_number', drug_number)
                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[6]/text()' % (i + 2))[0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = '2000-01-01'
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            hospital_name = ''
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = ''
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[1]/text()' % (i + 2))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                        update_time = create_time

                        try:
                            # 单价
                            drug_price = ''
                        except:
                            drug_price = ''

                        table_name = 'order_metadata_weizhouyingte'

                        try:
                            # 商品id
                            goods_id = data_html_cai.xpath(
                                '//*[@id="gvPurchaseList"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                        except:
                            goods_id = ''

                        drug_hashs = "%s %s %s %s" % (drug_name, drug_specification, delivery_id, supplier_name)
                        md5 = hashlib.md5()
                        md5.update(bytes(drug_hashs, encoding="utf-8"))
                        drug_hash = md5.hexdigest()
                        hospital_hashs = "%s %s %s" % (delivery_id, hospital_name, hospital_address)
                        md5 = hashlib.md5()
                        md5.update(bytes(hospital_hashs, encoding="utf-8"))
                        hospital_hash = md5.hexdigest()
                        stream_hashs = "%s %s %s %s %s %s %s %s %s %s" % (
                            company_id, delivery_id, bill_type, drug_hash, drug_unit, abs(drug_number), drug_batch,
                            valid_till,
                            hospital_hash, sell_time)
                        md5 = hashlib.md5()
                        md5.update(bytes(stream_hashs, encoding="utf-8"))
                        stream_hash = md5.hexdigest()
                        month = int(str(self.fist).replace('-', '')[0: 6])

                        sql_crm = "insert into order_metadata_weizhouyingte(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, goods_id, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, sell_time, create_time, update_time, drug_price,
                                                      goods_id,
                                                      drug_hash, hospital_hash, stream_hash, month)
                        # print('sql_data', sql_data_crm)
                        try:
                            self.db.ping()
                        except pymysql.MySQLError:
                            self.db.connect()

                        try:
                            self.cursor.execute(sql_data_crm)
                            self.db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                        self.cursor.execute('select max(id) from order_metadata_weizhouyingte')
                        foreign_id = self.cursor.fetchone()[0]

                        sql_crm_data = SQL_CRM_DATA
                        sql_data_crm_data = sql_crm_data.format(company_id, delivery_id, delivery_name, table_name,
                                                                foreign_id,
                                                                data_version,
                                                                data_type, bill_type, drug_name, drug_specification,
                                                                supplier_name,
                                                                drug_hash, drug_unit, abs(drug_number), drug_batch,
                                                                valid_till,
                                                                hospital_name,
                                                                hospital_address, hospital_hash, month, sell_time,
                                                                stream_hash,
                                                                create_time, update_time)

                        try:
                            self.cursor.execute(sql_data_crm_data)
                            self.db.commit()
                            self.crm_cursor.execute(sql_data_crm_data)
                            self.crm_db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                try:
                    crm_request_data = {
                        'version': delivery_id + "-" + self.time_stamp,
                        'streamType': streamType,
                    }
                    html = requests.post(url=CRM_REQUEST_URL, data=crm_request_data, headers=self.headers, verify=False)
                    self.classify_success = json.loads(html.content.decode('utf-8'))['success']
                except:
                    print('爬虫调取后端接口错误')

                get_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                get_date = int(time.strftime("%Y%m%d", time.localtime()))
                get_status = 1
                if MONTHS == 0:
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_weizhouyingte WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_weizhouyingte WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '04-wenzhouyingte_a',
                                                            delivery_id + "-" + self.time_stamp, get_time,
                                                            get_date,
                                                            get_status, data_num, self.classify_success, remark,
                                                            create_time,
                                                            update_time)

                try:
                    self.cursor.execute(sql_data_crm_record)
                    self.db.commit()
                    self.crm_cursor.execute(sql_data_crm_record)
                    self.crm_db.commit()
                except Exception as e:
                    print('插入失败:%s  sql_data_crm_record:%s' % (e, sql_data_crm_record))

                sql_crm_version = SQL_CRM_VERSION
                sql_data_crm_version = sql_crm_version.format(delivery_id + "-" + self.time_stamp,
                                                              enterprise_name,
                                                              company_id, create_time, update_time, data_num,
                                                              remark)

                try:
                    self.cursor.execute(sql_data_crm_version)
                    self.db.commit()
                except Exception as e:
                    print('插入失败:%s  sql_data_crm_version:%s' % (e, sql_data_crm_version))

            except Exception as e:
                print('wenzhouyingte_a-登入失败:%s' % e)
                print('self.number', self.number)
                if self.number < 4:
                    self.parse('aa')
                else:
                    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    get_time = create_time
                    get_date = int(time.strftime("%Y%m%d", time.localtime()))
                    get_status = 2
                    if MONTHS == 0:
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_weizhouyingte WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_weizhouyingte WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '04-wenzhouyingte_a',
                                                                delivery_id + "-" + self.time_stamp, get_time, get_date,
                                                                get_status, data_num, self.classify_success, remark,
                                                                create_time, update_time)

                    try:
                        self.cursor.execute(sql_data_crm_record)
                        self.db.commit()
                        self.crm_cursor.execute(sql_data_crm_record)
                        self.crm_db.commit()
                    except Exception as e:
                        print('插入失败:%s  sql_data_crm_record:%s' % (e, sql_data_crm_record))
                    print('账号密码或者验证码错误')
