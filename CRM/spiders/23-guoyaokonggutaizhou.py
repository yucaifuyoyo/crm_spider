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
from scrapy.selector import Selector
import random
import json
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE


class GuokonglishuiSpider(scrapy.Spider):
    name = 'guoyaokonggutaizhou'
    allowed_domains = ['.com']
    start_urls = ['http://www.gkzj.com:81/#']
    headers = HEADERS
    yesterday = YESTERDAY
    fist = FIST
    # fist = '2018-01-01'
    # last = '2018-11-05'
    last = LAST
    headers_post = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Content-Type": "application/json; charset=utf-8",
    }
    sess_guoyaokonggutaizhou = requests.session()
    time_stamp = str(int(time.time() * 1000))
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

    def parse(self, response):
        # delivery_id = 'F617B115D6F3447983E94BB781231287'
        delivery_id = 'DDA1001015'
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
            html = self.sess_guoyaokonggutaizhou.get(url=self.start_urls[0], headers=self.headers, verify=False)

            data = {
                "usercode": get_account,
                "password": get_pwd,
                "ownerid": "2",
            }
            post_html = self.sess_guoyaokonggutaizhou.post(url='http://www.gkzj.com:81/modal/login.php', data=data,
                                                           headers=self.headers,
                                                           verify=False)
            # print('*' * 1000)
            # print('post_html', json.loads(post_html.content.decode('utf-8'))['data']['token'])
            # print('*' * 1000)
            token = json.loads(post_html.content.decode('utf-8'))['data']['token']

            data_data = {
                "token": token,
                "goodid": "",
                "bgnDate": self.fist,
                "endDate": self.last,
                "dname": "",
                "page": "1",
                "rows": "50",
                "sort": "CREATEDATE",
                "order": "asc",
            }
            data_html = self.sess_guoyaokonggutaizhou.post(url='http://www.gkzj.com:81/modal/order.json.php',
                                                           data=data_data,
                                                           headers=self.headers,
                                                           verify=False)
            # print('*' * 1000)
            # print('data_html', json.loads(data_html.content.decode('utf-8')))
            # print('*' * 1000)
            try:
                data_html = json.loads(data_html.content.decode('utf-8'))
                data_num = int(int(data_html['total']) / 50) + 3
                for i in range(1, data_num):
                    data_data = {
                        "token": token,
                        "goodid": "",
                        # "bgnDate": "2018-08-01",
                        "bgnDate": self.fist,
                        # "endDate": "2018-10-26",
                        "endDate": self.last,
                        "dname": "",
                        "page": i,
                        "rows": "50",
                        "sort": "CREATEDATE",
                        "order": "asc",
                    }
                    data_html = self.sess_guoyaokonggutaizhou.post(url='http://www.gkzj.com:81/modal/order.json.php',
                                                                   data=data_data,
                                                                   headers=self.headers,
                                                                   verify=False)
                    # print('*' * 1000)
                    # print('data_html', json.loads(data_html.content.decode('utf-8')))
                    # print('*' * 1000)

                    md5 = hashlib.md5()
                    for data_resps in json.loads(data_html.content.decode('utf-8'))['rows']:
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
                            drug_name = data_resps['GOODSNAME']
                            if drug_name == 'None':
                                drug_name = 1
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = data_resps['SPEC'].strip()
                                if not drug_specification:
                                    drug_specification = ''
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps['PRODUCER'].strip()
                                if not supplier_name:
                                    supplier_name = ''
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = data_resps['MSUNITNO'].strip()
                                if not drug_unit:
                                    drug_unit = ''
                            except:
                                drug_unit = ''

                            try:
                                # 销售类型
                                bill_types = data_resps['FLOWNAME'].strip()
                                # print('bill_types', bill_types)
                                if '采购订单' in bill_types:
                                    bill_type = 1
                                elif '销售退货' in bill_types:
                                    bill_type = 4
                                elif '采购退货' in bill_types:
                                    bill_type = 2
                                elif '销售单据更正' in bill_types:
                                    bill_type = 5
                                else:
                                    bill_type = 3

                                # print('bill_type', bill_type)

                            except:
                                bill_type = 3

                            try:
                                # 出库数量
                                drug_number = data_resps['SALQTY'].strip()
                                if not drug_number:
                                    drug_number = data_resps['PURQTY'].strip()
                                    if not drug_number:
                                        drug_number = 0

                                # print('drug_number', drug_number)
                                # print('data_resps', data_resps)

                            except:
                                drug_number = 0

                            try:
                                # 医院类型
                                hospital_type = data_resps['CSTTYPE'].strip()
                                if not hospital_type:
                                    hospital_type = ''
                            except:
                                hospital_type = ''

                            try:
                                # 批号
                                drug_batch = data_resps['LOTNO'].strip()
                                if not drug_batch:
                                    drug_batch = ''
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_resps['ENDDATE'].strip()
                                if not valid_till:
                                    valid_till = '2000-01-01'
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 医院(终端)名称
                                if bill_type == 1 or bill_type == 2:
                                    hospital_name = ''
                                else:
                                    hospital_name = data_resps['DNAME'].strip()
                                    if not hospital_name:
                                        hospital_name = ''
                            except:
                                hospital_name = ''

                            try:
                                # 医院(终端)地址
                                if bill_type == 1 or bill_type == 2:
                                    hospital_address = ''
                                else:
                                    hospital_address = data_resps['SENDADDR'].strip()
                                    if not hospital_address:
                                        hospital_address = ''
                            except:
                                hospital_address = ''

                            try:
                                # 销售(制单)时间
                                sell_time = data_resps['CREATEDATE'].strip()
                                if not sell_time:
                                    sell_time = '2000-01-01'
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_guoyaokonggutaizhou'

                            try:
                                # 单价
                                drug_price = data_resps['PRC'].strip()
                                if not drug_price:
                                    drug_price = ''
                            except:
                                drug_price = ''
                            try:
                                # 供应商名称
                                channel_name = data_resps['PRODUCER'].strip()
                                if not channel_name:
                                    channel_name = ''
                            except:
                                channel_name = ''

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

                            sql_crm = "insert into order_metadata_guoyaokonggutaizhou(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, hospital_type, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, channel_name, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type, drug_name, drug_specification, supplier_name,
                                                          drug_unit,
                                                          abs(drug_number), hospital_type, drug_batch, valid_till,
                                                          hospital_name,
                                                          hospital_address, sell_time, create_time, update_time,
                                                          drug_price,
                                                          channel_name, drug_hash,
                                                          hospital_hash, stream_hash, month)
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

                            self.cursor.execute('select max(id) from order_metadata_guoyaokonggutaizhou')
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
                                if bill_type != 5:
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
                        "SELECT count(*) from order_metadata_guoyaokonggutaizhou WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_guoyaokonggutaizhou WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '23-guoyaokonggutaizhou',
                                                            delivery_id + "-" + self.time_stamp, get_time,
                                                            get_date,
                                                            get_status, data_num, self.classify_success, remark,
                                                            create_time,
                                                            update_time)

                try:
                    self.db.ping()
                except pymysql.MySQLError:
                    self.db.connect()

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
                print('guoyaokonggutaizhou-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_guoyaokonggutaizhou WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_guoyaokonggutaizhou WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '23-guoyaokonggutaizhou',
                                                                delivery_id + "-" + self.time_stamp, get_time, get_date,
                                                                get_status, data_num, self.classify_success, remark,
                                                                create_time, update_time)

                    try:
                        self.db.ping()
                    except pymysql.MySQLError:
                        self.db.connect()

                    try:
                        self.cursor.execute(sql_data_crm_record)
                        self.db.commit()
                        self.crm_cursor.execute(sql_data_crm_record)
                        self.crm_db.commit()
                    except Exception as e:
                        print('插入失败:%s  sql_data_crm_record:%s' % (e, sql_data_crm_record))
                    print('账号密码或者验证码错误')
