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


class JinhuayinteSpider(scrapy.Spider):
    name = 'dade'
    allowed_domains = ['.com']
    start_urls = ['http://www.dadeyy.com/cn/login.asp']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    sess_dade = requests.session()
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
        # delivery_id = 'F617B115D6F3447983E94BB781231256'
        delivery_id = 'DDA100100P'
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
            html = self.sess_dade.get(url=self.start_urls[0], headers=self.headers, verify=False)
            data = {"username": get_account,
                    "userPassword": get_pwd,
                    "lx": '1',
                    "Login": '登陆',
                    }
            # print('data', data)
            self.sess_dade.post(url="http://www.dadeyy.com/cn/check.asp",
                                data=data, headers=self.headers, verify=False)
            data_html = self.sess_dade.get(url='http://www.dadeyy.com/cn/ywmanage.asp?username=11868',
                                           headers=self.headers,
                                           verify=False)
            try:
                re.findall(r'日期', data_html.content.decode('gb2312', 'ignore'))[0]
                # print("data_html.content.decode('utf-8')", data_html.content.decode('gb2312', 'ignore'))
                post_data = {"start": self.fist,
                             "end": self.last,
                             "Submit": '查询',
                             }
                data_html = self.sess_dade.post(url="http://www.dadeyy.com/cn/ywmanage.asp?username=11868",
                                                data=post_data, headers=self.headers, verify=False)
                # print("data_html.content.decode('utf-8')", data_html.content.decode('gb2312', 'ignore'))
                data_html = etree.HTML(data_html.content.decode('gb2312', 'ignore'))
                # /html/body/table[3]/tr[1]
                data_len = int(len(data_html.xpath('/html/body/table[3]/tr'))) - 1
                # print('data_len', data_len)
                md5 = hashlib.md5()
                for i in range(data_len):
                    # try:
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
                            '/html/body/table[3]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                        if not drug_name:
                            drug_name = 1
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品id
                            trade_id = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                        except:
                            trade_id = ''

                        try:
                            # 药品规格
                            drug_specification = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[5]/text()' % (i + 2))[0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[6]/text()' % (i + 2))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 销售单id
                            sales_ticket_id = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[12]/text()' % (i + 2))[0].strip()
                        except:
                            sales_ticket_id = ''

                        try:
                            # 销售类型
                            bill_types = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[11]/text()' % (i + 2))[0].strip()
                            if '销售出库' in bill_types:
                                bill_type = 3
                            elif '采购入库' in bill_types:
                                bill_type = 1
                            elif '销售补差价' in bill_types:
                                bill_type = 3
                            elif '采购退出' in bill_types:
                                bill_type = 2
                            elif '销售退货' in bill_types:
                                bill_type = 4
                            else:
                                bill_type = 3
                        except:
                            bill_type = 3

                        try:
                            # 出库数量
                            drug_number = round(float(data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()))
                            if drug_number < 0:
                                bill_type = 4

                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[7]/text()' % (i + 2))[0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = '2000-01-01'
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            if bill_type == 1 or bill_type == 2:
                                hospital_name = ''
                            else:
                                hospital_name = data_html.xpath(
                                    '/html/body/table[3]/tr[%s]/td[10]/text()' % (i + 2))[0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = ''
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[1]/text()' % (i + 2))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 价格
                            drug_price = data_html.xpath(
                                '/html/body/table[3]/tr[%s]/td[9]/text()' % (i + 2))[0].strip()
                        except:
                            drug_price = ''

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_dade'

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

                        sql_crm = "insert into order_metadata_dade(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, trade_id, sales_ticket_id, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_hash, hospital_hash, stream_hash, drug_price, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, trade_id, sales_ticket_id,
                                                      drug_specification, supplier_name, drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, sell_time, create_time,
                                                      update_time,
                                                      drug_hash, hospital_hash, stream_hash, drug_price, month)
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

                        self.cursor.execute('select max(id) from order_metadata_dade')
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
                        "SELECT count(*) from order_metadata_dade WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_dade WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '14-dade',
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
                print('dade-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_dade WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_dade WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '14-dade',
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
