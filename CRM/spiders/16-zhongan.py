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
    name = 'zhongan'
    allowed_domains = ['.com']
    start_urls = ['http://www.zayy.cn/os/Default.aspx']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    sess_zhongan = requests.session()
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
        # delivery_id = 'F617B115D6F3447983E94BB781231258'
        delivery_id = 'DDA100100R'
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
            html = self.sess_zhongan.get(url=self.start_urls[0], headers=self.headers, verify=False)
            # print('html', html.content.decode('gb2312'))
            resp = etree.HTML(html.content.decode('utf-8', 'ignorg'))
            __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __VIEWSTATEGENERATOR = resp.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value')[0]
            image = self.sess_zhongan.get(url='http://www.zayy.cn/os/tools/VerifyCode1.aspx?', headers=self.headers,
                                          verify=False)
            # print('dict_from_cookiejar(image.cookies)', dict_from_cookiejar(image.cookies))
            if SCRAPYD_TYPE == 1:
                if 'indow' in platform.system():
                    symbol = r'\\'
                else:
                    symbol = r'/'
                path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                # print('path', path)
                files = r'{}{}static{}16-zhongan'.format(path, symbol, symbol)
                if not os.path.exists(files):
                    os.makedirs(files)
                with open(r'{}{}static{}16-zhongan{}yzm.jpg'.format(path, symbol, symbol, symbol), 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'{}{}static{}16-zhongan{}yzm.jpg'.format(path, symbol, symbol, symbol)
            else:
                with open(r'./16-zhonganyzm.jpg', 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'./16-zhonganyzm.jpg'

            codetype = 4005
            # 超时时间，秒
            timeout = 60
            ydm = YDMHttp()
            cid, code_result = ydm.run(filename, codetype, timeout)
            # yzm = input('请输入验证码：')
            # print('cid:%s   code_result:%s' % (cid, code_result))
            yzm = code_result
            # yzm = input('请输入验证码:')

            data = {
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "username": get_account,
                "userpwd": get_pwd,
                "verifycode": yzm,
                "ImgBtnLogin.x": "0",
                "ImgBtnLogin.y": "0",
            }
            post_html = self.sess_zhongan.post(url='http://www.zayy.cn/os/Default.aspx', data=data,
                                               headers=self.headers,
                                               verify=False)
            # print('data', data)
            # print('*' * 1000)
            # print('post_html', post_html.content.decode('utf-8', 'ignore'))
            # post_html = self.sess_zhongan.get(url='http://www.zayy.cn/os/UserLiuxiang.aspx?time1=2017-11-01&time2=2018-11-01&titlename=&pihao=&page=2',
            post_html = self.sess_zhongan.get(
                url='http://www.zayy.cn/os/UserLiuxiang.aspx?time1={}&time2={}&titlename=&pihao='.format(self.fist,
                                                                                                         self.last),
                headers=self.headers, verify=False)
            try:
                re.findall(r'出库时间', post_html.content.decode('utf-8', 'ignore'))[0]
                # print('*' * 1000)
                # print('post_html', post_html.content.decode('utf-8', 'ignore'))
                data_get = etree.HTML(post_html.content.decode('utf-8', 'ignore'))
                try:
                    page_num = int(data_get.xpath('//*[@id="ctl00_ContentPlaceHolder1_AspNetPager1"]/a/text()')[-3]) + 1
                except:
                    page_num = 2
                for i in range(1, page_num):
                    data_html = self.sess_zhongan.get(
                        url='http://www.zayy.cn/os/UserLiuxiang.aspx?time1={}&time2={}&titlename=&pihao=&page={}'.format(
                            self.fist, self.last, i),
                        headers=self.headers, verify=False)
                    # print('*' * 1000)
                    # print('data_html', data_html.content.decode('utf-8', 'ignore'))
                    data_resps = etree.HTML(data_html.content.decode('utf-8', 'ignore'))
                    # //*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tbody/tr[1]
                    data_len = int(len(data_resps.xpath('//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr'))) - 1
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
                            drug_name = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[4]/span/text()' % (i + 2))[
                                0].strip()
                            if not drug_name:
                                drug_name = 1
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[5]/span/text()' % (
                                            i + 2))[0].strip()
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[7]/span/text()' % (
                                            i + 2))[0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[6]/span/text()' % (
                                            i + 2))[0].strip()
                            except:
                                drug_unit = ''

                            try:
                                # 出库数量
                                drug_number = int(
                                    data_resps.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[10]/span/text()' % (
                                                i + 2))[0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[8]/span/text()' % (
                                            i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[9]/span/text()' % (
                                            i + 2))[
                                    0].strip()
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 医院(终端)名称
                                hospital_name = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[3]/span/text()' % (
                                            i + 2))[0].strip()
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
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[2]/span/text()' % (
                                            i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_zhongan'

                            try:
                                # 单价
                                drug_price = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[11]/span/text()' % (
                                            i + 2))[0].strip()
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

                            sql_crm = "insert into order_metadata_zhongan(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
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
                                self.cursor.execute(sql_data_crm)
                                self.db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                            self.cursor.execute('select max(id) from order_metadata_zhongan')
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

                    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    data_html = self.sess_zhongan.get(
                        url='http://www.zayy.cn/os/UserDataquery.aspx?time1={}&time2={}&titlename=&pihaotxt='.format(
                            self.fist, self.last),
                        headers=self.headers, verify=False)
                    # print('*' * 1000)
                    # print('data_html', data_html.content.decode('utf-8', 'ignore'))
                    data_resps = etree.HTML(data_html.content.decode('utf-8', 'ignore'))
                    # //*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tbody/tr[1]
                    data_len = int(len(data_resps.xpath('//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr'))) - 1
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
                                '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[4]/span/text()' % (i + 2))[
                                0].strip()
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
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[6]/span/text()' % (
                                            i + 2))[0].strip()
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
                                        '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[8]/text()' % (
                                                i + 2))[0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[5]/span/text()' % (
                                            i + 2))[
                                    0].strip()
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
                                sell_time = data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_DGProduct"]/tr[%s]/td[9]/span/text()' % (
                                            i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_zhongan'

                            try:
                                # 单价
                                drug_price = ''
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

                            sql_crm = "insert into order_metadata_zhongan(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
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
                                self.cursor.execute(sql_data_crm)
                                self.db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                            self.cursor.execute('select max(id) from order_metadata_zhongan')
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
                        "SELECT count(*) from order_metadata_zhongan WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_zhongan WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '16-zhongan',
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
                print('zhongan-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_zhongan WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_zhongan WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '16-zhongan',
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
