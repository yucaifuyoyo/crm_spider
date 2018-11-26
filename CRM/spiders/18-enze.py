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
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE


class NingboyinteSpider(scrapy.Spider):
    name = 'enze'
    allowed_domains = ['.com']
    start_urls = ['http://kh.zjezyy.com/']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    # print('FIST', FIST)
    # fist = YESTERDAY
    # fist = '2018-08-01'
    fist_year = int(str(fist)[0: 4])
    fist_month = int(str(fist)[5: 7])
    fist_day = int(str(fist)[8: 10])
    fists = 1
    if fist_day == 31:
        fists = 0
        fist_day = fist_day - 1
    last = LAST
    # print('LAST', LAST)
    # last = YESTERDAY
    # last = '2018-08-31'
    # last = '2018-09-30'
    last_year = int(str(last)[0: 4])
    last_month = int(str(last)[5: 7])
    last_day = int(str(last)[8: 10])
    lasts = 1
    if last_day == 31:
        lasts = 0
        last_month = last_month + 1
        last_day = 1
        if last_month == 13:
            last_month = 1

    sess_enze = requests.session()
    headers_post = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Content-Type": "text/plain; charset=utf-8",
    }
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
        # delivery_id = 'F617B115D6F3447983E94BB781231264'
        delivery_id = 'DDA100100T'
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
            html = self.sess_enze.get(url=self.start_urls[0], headers=self.headers, verify=False)

            data = {
                "code": "1         ",
                "password": get_pwd,
                # "password": 'aaaaa',
                "usename": get_account,
            }
            # post_html = self.sess_enze.post(url='http://kh.zjezyy.com/ajaxpro/Dengru,ezyyuser.ashx', data=json.dumps(data), headers=self.headers_post,
            post_html_a = self.sess_enze.post(url='http://kh.zjezyy.com/ajaxpro/Dengru,ezyyuser.ashx', data=data,
                                              headers=self.headers_post,
                                              verify=False)
            # print('data', data)
            # print('*' * 1000)
            # print('post_html', post_html_a.content.decode('utf-8', 'ignore'))
            post_html_b = self.sess_enze.get(url='http://kh.zjezyy.com/Defaultlast.aspx?transmissionInfo=4674,1',
                                             headers=self.headers, verify=False)
            try:
                re.findall(r'杨舜杰', post_html_b.content.decode('utf-8', 'ignore'))
                # print('post_html', post_html_b.content.decode('utf-8', 'ignore'))
                # print('*' * 1000)
                post_html_c = self.sess_enze.get(url='http://kh.zjezyy.com/GYS/SPLX.aspx',
                                                 headers=self.headers, verify=False)
                # print('post_html', post_html_c.content.decode('utf-8', 'ignore'))
                # print('*' * 1000)
                resp = etree.HTML(post_html_c.content.decode('utf-8', 'ignorg'))
                __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                DropDownListSp_list = resp.xpath('//*[@id="DropDownListSp"]/option/@value')
                drug_name_list = resp.xpath('//*[@id="DropDownListSp"]/option/text()')
                # print('-' * 1000)
                # print('DropDownListSp_list', DropDownListSp_list)
                # print('drug_name_list', drug_name_list)
                # print('-'*1000)
                for j in range(len(DropDownListSp_list)):
                    time_data = {
                        "__EVENTARGUMENT": "",
                        "__EVENTTARGET": "WucDateTime1$DropDownList1",
                        # "__EVENTTARGET": "WucDateTime1$DropDownList2",
                        "__EVENTVALIDATION": __EVENTVALIDATION,
                        "__LASTFOCUS": "",
                        "__VIEWSTATE": __VIEWSTATE,
                        "DropDownListSp": DropDownListSp_list[j],
                        "WucDateTime1$DropDownList1": self.fist_year,
                        "WucDateTime1$DropDownList2": self.fist_month,
                        "WucDateTime1$DropDownList3": self.fist_day,
                        "WucDateTime2$DropDownList1": self.last_year,
                        "WucDateTime2$DropDownList2": self.last_month,
                        "WucDateTime2$DropDownList3": self.last_day,
                    }
                    post_html_b = self.sess_enze.post(url='http://kh.zjezyy.com/GYS/SPLX.aspx', data=time_data,
                                                      headers=self.headers, verify=False)
                    # print('post_html', post_html_b.content.decode('utf-8', 'ignore'))
                    # print('*' * 1000)
                    # print('time_data', time_data)
                    resp = etree.HTML(post_html_b.content.decode('utf-8', 'ignorg'))
                    __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                    __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                    time_data = {
                        "__EVENTARGUMENT": "",
                        "__EVENTTARGET": "",
                        "__EVENTVALIDATION": __EVENTVALIDATION,
                        "__LASTFOCUS": "",
                        "__VIEWSTATE": __VIEWSTATE,
                        "Button1": "检索",
                        "DropDownListSp": DropDownListSp_list[j],
                        "WucDateTime1$DropDownList1": self.fist_year,
                        "WucDateTime1$DropDownList2": self.fist_month,
                        "WucDateTime1$DropDownList3": self.fist_day,
                        "WucDateTime2$DropDownList1": self.last_year,
                        "WucDateTime2$DropDownList2": self.last_month,
                        "WucDateTime2$DropDownList3": self.last_day,

                    }
                    # print('time_data', time_data)
                    data_html = self.sess_enze.post(url='http://kh.zjezyy.com/GYS/SPLX.aspx', data=time_data,
                                                    headers=self.headers, verify=False)
                    # print('data_html', data_html.content.decode('utf-8', 'ignore'))
                    # print('*'*1000)
                    data_resps = etree.HTML(data_html.content.decode('utf-8', 'ignore'))
                    # //*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tbody/tr[1]
                    data_len = int(len(data_resps.xpath('//*[@id="GridView1"]/tr'))) - 1
                    # print('*' * 1000)
                    # print(data_len)
                    # print('*' * 1000)
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
                            drug_name = drug_name_list[j].split('_')[0]
                            if not drug_name:
                                drug_name = 1
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[10]/text()' % (i + 2))[0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = ''
                            except:
                                drug_unit = ''

                            try:
                                # 出库数量
                                drug_number = int(
                                    data_resps.xpath(
                                        '//*[@id="GridView1"]/tr[%s]/td[6]/text()' % (
                                                i + 2))[0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[5]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[8]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 有效期至
                                bill_types = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[9]/text()' % (i + 2))[
                                    0].strip()
                                if '销售复核' in bill_types:
                                    bill_type = 3
                                elif '销售退货验收' in bill_types:
                                    bill_type = 4
                                else:
                                    bill_type = 3
                            except:
                                bill_type = 3

                            try:
                                # 医院(终端)名称
                                hospital_name = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                            except:
                                hospital_name = ''

                            try:
                                # 医院(终端)地址
                                hospital_address = ''
                            except:
                                hospital_address = ''

                            try:
                                # 销售(制单)时间
                                sell_time = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_enze'

                            try:
                                # 单价
                                drug_price = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[7]/text()' % (i + 2))[0].strip()
                            except:
                                drug_price = ''

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

                            sql_crm = "insert into order_metadata_enze(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type, drug_name, drug_specification, supplier_name,
                                                          drug_unit,
                                                          abs(drug_number), drug_batch, valid_till, hospital_name,
                                                          hospital_address, sell_time, create_time, update_time,
                                                          drug_price,
                                                          drug_hash,
                                                          hospital_hash, stream_hash, month)
                            # print('sql_data', sql_data_crm)
                            try:
                                self.db.ping()
                            except pymysql.MySQLError:
                                self.db.connect()
                            # print('MONTHS', MONTHS)
                            # print('sell_time', sell_time)
                            # print('sell_time', str(self.fist_year) + '-' + str(self.fist_month))
                            try:
                                if MONTHS == 0:
                                    if self.fists == 1:
                                        self.cursor.execute(sql_data_crm)
                                        self.db.commit()
                                    else:
                                        if '31' in sell_time:
                                            self.cursor.execute(sql_data_crm)
                                            self.db.commit()
                                else:
                                    if self.lasts == 1:
                                        self.cursor.execute(sql_data_crm)
                                        self.db.commit()
                                    else:
                                        if (str(self.fist)[0: 4] + '-' + str(self.fist)[5: 7]) in sell_time:
                                            self.cursor.execute(sql_data_crm)
                                            self.db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                            self.cursor.execute('select max(id) from order_metadata_enze')
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
                                    if MONTHS == 0:
                                        if self.fists == 1:
                                            self.cursor.execute(sql_data_crm_data)
                                            self.db.commit()
                                            self.crm_cursor.execute(sql_data_crm_data)
                                            self.crm_db.commit()
                                        else:
                                            if '31' in sell_time:
                                                self.cursor.execute(sql_data_crm_data)
                                                self.db.commit()
                                                self.crm_cursor.execute(sql_data_crm_data)
                                                self.crm_db.commit()
                                    else:
                                        if self.lasts == 1:
                                            self.cursor.execute(sql_data_crm_data)
                                            self.db.commit()
                                            self.crm_cursor.execute(sql_data_crm_data)
                                            self.crm_db.commit()
                                        else:
                                            if (str(self.fist)[0: 4] + '-' + str(self.fist)[5: 7]) in sell_time:
                                                self.cursor.execute(sql_data_crm_data)
                                                self.db.commit()
                                                self.crm_cursor.execute(sql_data_crm_data)
                                                self.crm_db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                post_html_c = self.sess_enze.get(url='http://kh.zjezyy.com/GYS/EZGR.aspx',
                                                 headers=self.headers, verify=False)
                # print('post_html', post_html_c.content.decode('utf-8', 'ignore'))
                # print('*' * 1000)
                resp = etree.HTML(post_html_c.content.decode('utf-8', 'ignorg'))
                __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                DropDownListSp_list = resp.xpath('//*[@id="DropDownListSp"]/option/@value')
                drug_name_list = resp.xpath('//*[@id="DropDownListSp"]/option/text()')
                # print('-' * 1000)
                # print('DropDownListSp_list', DropDownListSp_list)
                # print('drug_name_list', drug_name_list)
                # print('-' * 1000)
                for j in range(len(DropDownListSp_list)):
                    time_data = {
                        "__EVENTARGUMENT": "",
                        "__EVENTTARGET": "WucDateTime1$DropDownList1",
                        "__EVENTVALIDATION": __EVENTVALIDATION,
                        "__LASTFOCUS": "",
                        "__VIEWSTATE": __VIEWSTATE,
                        "DropDownListSp": DropDownListSp_list[j],
                        "WucDateTime1$DropDownList1": self.fist_year,
                        "WucDateTime1$DropDownList2": self.fist_month,
                        "WucDateTime1$DropDownList3": self.fist_day,
                        "WucDateTime2$DropDownList1": self.last_year,
                        "WucDateTime2$DropDownList2": self.last_month,
                        "WucDateTime2$DropDownList3": self.last_day,
                    }
                    post_html_b = self.sess_enze.post(url='http://kh.zjezyy.com/GYS/EZGR.aspx', data=time_data,
                                                      headers=self.headers, verify=False)
                    # print('post_html', post_html_b.content.decode('utf-8', 'ignore'))
                    # print('*' * 1000)
                    # print('time_data', time_data)
                    resp = etree.HTML(post_html_b.content.decode('utf-8', 'ignorg'))
                    __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                    __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                    time_data = {
                        "__EVENTARGUMENT": "",
                        "__EVENTTARGET": "",
                        "__EVENTVALIDATION": __EVENTVALIDATION,
                        "__LASTFOCUS": "",
                        "__VIEWSTATE": __VIEWSTATE,
                        "Button1": "检索",
                        "DropDownListSp": DropDownListSp_list[j],
                        "WucDateTime1$DropDownList1": self.fist_year,
                        "WucDateTime1$DropDownList2": self.fist_month,
                        "WucDateTime1$DropDownList3": self.fist_day,
                        "WucDateTime2$DropDownList1": self.last_year,
                        "WucDateTime2$DropDownList2": self.last_month,
                        "WucDateTime2$DropDownList3": self.last_day,

                    }
                    data_html = self.sess_enze.post(url='http://kh.zjezyy.com/GYS/EZGR.aspx', data=time_data,
                                                    headers=self.headers, verify=False)
                    # print('--------', data_html.content.decode('utf-8', 'ignore'))
                    data_resps = etree.HTML(data_html.content.decode('utf-8', 'ignore'))
                    # //*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tbody/tr[1]
                    data_len = int(len(data_resps.xpath('//*[@id="GridView1"]/tr'))) - 1
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
                            drug_name = data_resps.xpath(
                                '//*[@id="GridView1"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                            if not drug_name:
                                drug_name = 1
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = ''
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = ''
                            except:
                                drug_unit = ''

                            try:
                                # 出库数量
                                drug_number = int(
                                    data_resps.xpath(
                                        '//*[@id="GridView1"]/tr[%s]/td[5]/text()' % (
                                                i + 2))[0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[4]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = '2000-01-01'
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 有效期至
                                bill_types = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[7]/text()' % (i + 2))[
                                    0].strip()
                                if '采购退回' in bill_types:
                                    bill_type = 2
                                elif '采购' in bill_types:
                                    bill_type = 1
                                else:
                                    bill_type = 1
                            except:
                                bill_type = 1

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
                                sell_time = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_enze'

                            try:
                                # 单价
                                drug_price = data_resps.xpath(
                                    '//*[@id="GridView1"]/tr[%s]/td[6]/text()' % (i + 2))[0]
                            except:
                                drug_price = ''

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

                            sql_crm = "insert into order_metadata_enze(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type, drug_name, drug_specification, supplier_name,
                                                          drug_unit,
                                                          abs(drug_number), drug_batch, valid_till, hospital_name,
                                                          hospital_address, sell_time, create_time, update_time,
                                                          drug_price,
                                                          drug_hash,
                                                          hospital_hash, stream_hash, month)
                            # print('sql_data', sql_data_crm)
                            try:
                                self.db.ping()
                            except pymysql.MySQLError:
                                self.db.connect()

                            try:
                                if MONTHS == 0:
                                    if self.fists == 1:
                                        self.cursor.execute(sql_data_crm)
                                        self.db.commit()
                                    else:
                                        if '31' in sell_time:
                                            self.cursor.execute(sql_data_crm)
                                            self.db.commit()
                                else:
                                    if self.lasts == 1:
                                        self.cursor.execute(sql_data_crm)
                                        self.db.commit()
                                    else:
                                        if (str(self.fist)[0: 4] + '-' + str(self.fist)[5: 7]) in sell_time:
                                            self.cursor.execute(sql_data_crm)
                                            self.db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                            self.cursor.execute('select max(id) from order_metadata_enze')
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
                                    if MONTHS == 0:
                                        if self.fists == 1:
                                            self.cursor.execute(sql_data_crm_data)
                                            self.db.commit()
                                            self.crm_cursor.execute(sql_data_crm_data)
                                            self.crm_db.commit()
                                        else:
                                            if '31' in sell_time:
                                                self.cursor.execute(sql_data_crm_data)
                                                self.db.commit()
                                                self.crm_cursor.execute(sql_data_crm_data)
                                                self.crm_db.commit()
                                    else:
                                        if self.lasts == 1:
                                            self.cursor.execute(sql_data_crm_data)
                                            self.db.commit()
                                            self.crm_cursor.execute(sql_data_crm_data)
                                            self.crm_db.commit()
                                        else:
                                            if (str(self.fist)[0: 4] + '-' + str(self.fist)[5: 7]) in sell_time:
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
                        "SELECT count(*) from order_metadata_enze WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_enze WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '18-enze',
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
                print('enze-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_enze WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_enze WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '18-enze',
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
