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
from ..settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE
import platform


class HuirenSpider(scrapy.Spider):
    name = 'huiren'
    allowed_domains = ['.com']
    start_urls = ['http://www.zjhuiren.com/login.asp']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    sess_huiren = requests.session()
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
        delivery_id = 'DDA1001003'
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
            # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            login_url = "http://www.zjhuiren.com/login.asp"
            post_url = "http://www.zjhuiren.com/login.asp?action=loginsub"

            res1 = self.sess_huiren.get(login_url, headers=self.headers)
            selector = Selector(text=res1.text)
            # print(dict_from_cookiejar(res1.cookies))
            random_value = random.randint(1, 9)
            k = selector.css("input[name='codeKey']::attr(value)").extract_first()
            # print(k)

            code_url = "http://www.zjhuiren.com/DvCode.asp?k=%s&" % (k)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
                "Host": "www.zjhuiren.com",
                "Referer": "http://www.zjhuiren.com/login.asp"
            }
            res2 = self.sess_huiren.get(code_url, headers=headers)
            # print(res2.text)
            if SCRAPYD_TYPE == 1:
                if 'indow' in platform.system():
                    symbol = r'\\'
                else:
                    symbol = r'/'
                path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                files = r'{}{}static{}03-huiren'.format(path, symbol, symbol)
                if not os.path.exists(files):
                    os.makedirs(files)
                with open(r'{}{}static{}03-huiren{}yzm.jpg'.format(path, symbol, symbol, symbol), 'wb') as f:
                    f.write(res2.content)
                filename = r'{}{}static{}03-huiren{}yzm.jpg'.format(path, symbol, symbol, symbol)
            else:
                with open(r'./03-huirenyzm.jpg', 'wb') as f:
                    f.write(res2.content)
                filename = r'./03-huirenyzm.jpg'
            codetype = 4000
            # 超时时间，秒
            timeout = 60
            ydm = YDMHttp()
            result = ydm.run(filename, codetype, timeout)
            # print(result)
            code = result[1]
            # code = input("请输入code")

            post_data = {
                "UserID": get_account,
                "UserPass": get_pwd,
                "codeKey": k,
                "code": code,
                "B1": "提交"
            }
            res3 = self.sess_huiren.post(post_url, data=post_data, headers=headers)
            res4 = self.sess_huiren.get("http://www.zjhuiren.com/manager.asp?imark=1&ID=1888", headers=headers)

            data = {
                # "bgtime": "2018-10-01",
                "bgtime": self.fist,
                # "ovtime": "2018-10-15",
                "ovtime": self.last,
                "oldshow": "purchase",
                "px": "rq",
                "spbm": "",
                "tym": "",
                "dwmch": "",
            }

            data_resp = self.sess_huiren.post(url='http://www.zjhuiren.com/gjmx.asp?action=goselect&ID=1888', data=data,
                                              headers=self.headers,
                                              verify=False)
            # print('11' * 1000)
            # print(data_resp.content.decode('utf-8'))
            # print('11' * 1000)
            try:
                re.findall(r'日期', data_resp.content.decode('utf-8'))[0]
                try:
                    page = int(re.findall(r'【页次：1/(.+?)页】', data_resp.content.decode('utf-8'))[0]) + 1
                except:
                    page = 0
                # print('page', page)
                # time.sleep(10)
                for i in range(1, page):
                    data_resp = self.sess_huiren.get(
                        url='http://www.zjhuiren.com/gjmx.asp?Page={}&ID=1888&bgtime={}&ovtime={}&spbm=&tym=&dwmch=&px=rq&oldshow=purchase'.format(
                            i, self.fist, self.last),
                        # data_resp = self.sess_huiren.get(url='http://www.zjhuiren.com/gjmx.asp?Page={}&ID=1888&bgtime={}&ovtime={}&spbm=&tym=&dwmch=&px=rq&oldshow=purchase'.format(i, '2018-10-01', '2018-10-15'),
                        headers=self.headers,
                        verify=False)
                    # print('*' * 1000)
                    data_resps = etree.HTML(data_resp.content.decode('utf-8'))
                    data_len = data_resps.xpath('/html/body/table[2]/tr')
                    # print(data_len)
                    # print(len(data_len))
                    md5 = hashlib.md5()
                    for i in range(2, int(len(data_len))):
                        company_id = company_id
                        delivery_id = delivery_id
                        delivery_name = enterprise_name
                        data_version = delivery_id + "-" + self.time_stamp
                        data_type = 1
                        bill_type = 1
                        try:
                            drug_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[3]/text()' % i)[0].strip()
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                drug_specification = data_resps.xpath('/html/body/table[2]/tr[%s]/td[5]/text()' % i)[
                                    0].strip()
                            except:
                                drug_specification = ''

                            try:
                                supplier_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[6]/text()' % i)[
                                    0].strip()
                            except:
                                supplier_name = ''

                            try:
                                drug_unit = data_resps.xpath('/html/body/table[2]/tr[%s]/td[7]/text()' % i)[0].strip()
                            except:
                                drug_unit

                            try:
                                drug_number = int(
                                    data_resps.xpath('/html/body/table[2]/tr[%s]/td[8]/text()' % i)[0].strip())
                                if drug_number < 0:
                                    bill_type = 2
                            except:
                                drug_number = 0

                            try:
                                drug_batch = data_resps.xpath('/html/body/table[2]/tr[%s]/td[10]/text()' % i)[0].strip()
                            except:
                                drug_batch = ''
                            try:
                                valid_till = data_resps.xpath('/html/body/table[2]/tr[%s]/td[9]/text()' % i)[0].strip()
                                if not valid_till:
                                    valid_till = '2000-01-01'
                            except:
                                valid_till = '2000-01-01'

                            try:
                                if bill_type == 1 or bill_type == 2:
                                    hospital_name = ''
                                else:
                                    hospital_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[13]/text()' % i)[
                                        0].strip()
                            except:
                                hospital_name = ''

                            try:
                                hospital_address = ''
                            except:
                                hospital_address = ''

                            try:
                                sell_time = data_resps.xpath('/html/body/table[2]/tr[%s]/td[1]/text()' % i)[0].strip()
                            except:
                                sell_time = ''
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_huiren'
                            try:
                                drug_price = ''
                            except:
                                drug_price = ''
                            try:
                                drug_price_sum = ''
                            except:
                                drug_price_sum
                            try:
                                goods_id = data_resps.xpath('/html/body/table[2]/tr[%s]/td[2]/text()' % i)[0].strip()
                            except:
                                goods_id = ''
                            try:
                                trade_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[4]/text()' % i)[0].strip()
                            except:
                                trade_name = ''
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

                            sql_crm = "insert into order_metadata_huiren(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_price_sum, goods_id, trade_name, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type,
                                                          drug_name, drug_specification, supplier_name, drug_unit,
                                                          abs(drug_number),
                                                          drug_batch, valid_till, hospital_name, hospital_address,
                                                          sell_time,
                                                          create_time, update_time, drug_price, drug_price_sum,
                                                          goods_id, trade_name, drug_hash,
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

                            self.cursor.execute('select max(id) from order_metadata_huiren')
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
                                self.crm_db.ping()
                            except pymysql.MySQLError:
                                self.crm_db.connect()
                            try:
                                self.cursor.execute(sql_data_crm_data)
                                self.db.commit()
                                self.crm_cursor.execute(sql_data_crm_data)
                                self.crm_db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                data = {
                    # "bgtime": "2018-10-01",
                    "bgtime": self.fist,
                    # "ovtime": "2018-10-15",
                    "ovtime": self.last,
                    "oldshow": "sale",
                    "px": "rq",
                    "spbm": "",
                    "tym": "",
                    "dwmch": "",
                }

                data_resp = self.sess_huiren.post(url='http://www.zjhuiren.com/xsmx.asp?action=goselect&ID=1888',
                                                  data=data,
                                                  headers=self.headers,
                                                  verify=False)
                try:
                    page = int(re.findall(r'【页次：1/(.+?)页】', data_resp.content.decode('utf-8'))[0]) + 1
                except:
                    page = 1
                # print(page)
                # time.sleep(10)
                for i in range(1, page):
                    # url = 'http://www.zjhuiren.com/xsmx.asp?Page=%s&ID=1888&bgtime=2018-10-01&ovtime=2018-10-15&spbm=&tym=&dwmch=&px=rq&oldshow=sale' % i
                    url = 'http://www.zjhuiren.com/xsmx.asp?Page={}&ID=1888&bgtime={}&ovtime={}&spbm=&tym=&dwmch=&px=rq&oldshow=sale'.format(
                        i, self.fist, self.last)
                    # print(url)
                    data_resp = self.sess_huiren.get(url=url, headers=self.headers, verify=False)
                    # print('*' * 1000)
                    data_resps = etree.HTML(data_resp.content.decode('utf-8'))
                    data_len = data_resps.xpath('/html/body/table[2]/tr')
                    # print(data_len)
                    # print(len(data_len))
                    md5 = hashlib.md5()
                    for i in range(2, int(len(data_len))):
                        company_id = company_id
                        delivery_id = delivery_id
                        delivery_name = enterprise_name
                        data_version = delivery_id + "-" + self.time_stamp
                        data_type = 1
                        bill_type = 3
                        try:
                            drug_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[3]/text()' % i)[0].strip()
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                drug_specification = data_resps.xpath('/html/body/table[2]/tr[%s]/td[5]/text()' % i)[
                                    0].strip()
                            except:
                                drug_specification = ''

                            try:
                                supplier_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[6]/text()' % i)[
                                    0].strip()
                            except:
                                supplier_name = ''

                            try:
                                drug_unit = data_resps.xpath('/html/body/table[2]/tr[%s]/td[7]/text()' % i)[0].strip()
                            except:
                                drug_unit = ''

                            try:
                                drug_number = int(
                                    data_resps.xpath('/html/body/table[2]/tr[%s]/td[9]/text()' % i)[0].strip())
                                if drug_number < 0:
                                    bill_type = 4
                            except:
                                drug_number = 0

                            try:
                                drug_batch = data_resps.xpath('/html/body/table[2]/tr[%s]/td[8]/text()' % i)[0].strip()
                            except:
                                drug_batch = ''
                            try:
                                valid_till = data_resps.xpath('/html/body/table[2]/tr[%s]/td[12]/text()' % i)[0].strip()
                                if not valid_till:
                                    valid_till = '2000-01-01'
                            except:
                                valid_till = '2000-01-01'

                            try:
                                if bill_type == 1 or bill_type == 2:
                                    hospital_name = ''
                                else:
                                    hospital_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[13]/text()' % i)[
                                        0].strip()
                            except:
                                hospital_name = ''

                            try:
                                hospital_address = data_resps.xpath('/html/body/table[2]/tr[%s]/td[14]/text()' % i)[
                                    0].strip()
                            except:
                                hospital_address = ''

                            try:
                                sell_time = data_resps.xpath('/html/body/table[2]/tr[%s]/td[1]/text()' % i)[0].strip()
                            except:
                                sell_time = ''
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_huiren'
                            try:
                                drug_price = data_resps.xpath('/html/body/table[2]/tr[%s]/td[10]/text()' % i)[0].strip()
                            except:
                                drug_price = ''
                            try:
                                drug_price_sum = data_resps.xpath('/html/body/table[2]/tr[%s]/td[11]/text()' % i)[
                                    0].strip()
                            except:
                                drug_price_sum
                            try:
                                goods_id = data_resps.xpath('/html/body/table[2]/tr[%s]/td[2]/text()' % i)[0].strip()
                            except:
                                goods_id = ''
                            try:
                                trade_name = data_resps.xpath('/html/body/table[2]/tr[%s]/td[4]/text()' % i)[0].strip()
                            except:
                                trade_name = ''
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

                            sql_crm = "insert into order_metadata_huiren(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_price_sum, goods_id, trade_name, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type,
                                                          drug_name, drug_specification, supplier_name, drug_unit,
                                                          abs(drug_number),
                                                          drug_batch, valid_till, hospital_name, hospital_address,
                                                          sell_time,
                                                          create_time, update_time, drug_price, drug_price_sum,
                                                          goods_id, trade_name, drug_hash,
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

                            self.cursor.execute('select max(id) from order_metadata_huiren')
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
                                self.crm_db.ping()
                            except pymysql.MySQLError:
                                self.crm_db.connect()

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
                        "SELECT count(*) from order_metadata_huiren WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_huiren WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '03-huiren',
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
                print('huiren-登入失败:%s' % e)
                print('self.number', self.number)
                if self.number < 8:
                    self.parse('aa')
                else:
                    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    get_time = create_time
                    get_date = int(time.strftime("%Y%m%d", time.localtime()))
                    get_status = 2
                    if MONTHS == 0:
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_huiren WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_huiren WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '03-huiren',
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
