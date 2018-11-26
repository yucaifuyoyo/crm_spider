# -*- coding: utf-8 -*-
import os
import re
import datetime
import time
import hashlib

import scrapy
from scrapy.selector import Selector
import pymysql
import requests
from lxml import etree
import json
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE
from CRM.spiders.YDM import YDMHttp


class GuokaonSpider(scrapy.Spider):
    name = 'guokongwencheng'
    allowed_domains = ['wzshengwu.com']
    start_urls = ['http://data.wzshengwu.com/Login.aspx']
    sess_guokongwencheng = requests.Session()
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    # fist = '2017-09-01'
    last = LAST
    # last = '2018-10-31'
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
    __VIEWSTATE_jin = ''
    __VIEWSTATE_tui = ''
    CheckName = {}

    def parse(self, response):
        delivery_id = 'F617B115D6F3447983E94BB781231272'
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
            html = self.sess_guokongwencheng.get(url=self.start_urls[0], headers=self.headers, verify=False)
            resp = etree.HTML(html.content.decode('utf-8'))
            __LASTFOCUS = ''
            __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __VIEWSTATEGENERATOR = resp.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value')[0]
            __EVENTTARGET = ''
            __EVENTARGUMENT = ''
            __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]

            image = self.sess_guokongwencheng.get(url='http://data.wzshengwu.com/CheckCode.aspx', headers=self.headers,
                                                  verify=False)
            if SCRAPYD_TYPE == 1:
                if 'indow' in platform.system():
                    symbol = r'\\'
                else:
                    symbol = r'/'
                path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                # print('path', path)
                files = r'{}{}static{}22-guokongwencheng'.format(path, symbol, symbol)
                if not os.path.exists(files):
                    os.makedirs(files)
                with open(r'{}{}static{}22-guokongwencheng{}yzm.jpg'.format(path, symbol, symbol, symbol), 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'{}{}static{}22-guokongwencheng{}yzm.jpg'.format(path, symbol, symbol, symbol)
            else:
                with open(r'./22-guokongwenchengyzm.jpg', 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'./22-guokongwenchengyzm.jpg'

            codetype = 1004
            # 超时时间，秒
            timeout = 60
            ydm = YDMHttp()
            cid, code_result = ydm.run(filename, codetype, timeout)
            data = {
                "__LASTFOCUS": __LASTFOCUS,
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "__EVENTTARGET": __EVENTTARGET,
                "__EVENTARGUMENT": __EVENTARGUMENT,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "ddl": "2",
                "txtusername": get_account,
                "txtpassword": get_pwd,
                "txtverify": code_result,
                "btn_login.x": "41",
                "btn_login.y": "48",
            }

            # print('data', data)
            # data = urllib.parse.urlencode(data)

            uel = 'http://data.wzshengwu.com/Login.aspx?ReturnUrl=%2fSaleQuery.aspx'
            repost = self.sess_guokongwencheng.post(url=uel, data=data, headers=self.headers, verify=False)

            data_get = self.sess_guokongwencheng.get(url='http://data.wzshengwu.com/SaleQuery.aspx',
                                                     headers=self.headers, verify=False)
            try:
                data_get = etree.HTML(data_get.content.decode('utf-8'))
                __VIEWSTATE = data_get.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                # print('__VIEWSTATE:', __VIEWSTATE)
                CheckName_name = data_get.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView2"]/tr/td/input/@name')
                datas = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": __VIEWSTATE,
                    "__VIEWSTATEGENERATOR": "19E53B10",
                    "__SCROLLPOSITIONX": "0",
                    "__SCROLLPOSITIONY": "0",
                    "__VIEWSTATEENCRYPTED": "",
                    "ctl00$ContentPlaceHolder1$txtpage": "9999",  # 10000以内
                    # "ctl00$ContentPlaceHolder1$tbdatebegin": "2018-10-29",
                    "ctl00$ContentPlaceHolder1$tbdatebegin": self.fist,
                    # "ctl00$ContentPlaceHolder1$tbdateend": "2018-10-31",
                    "ctl00$ContentPlaceHolder1$tbdateend": self.last,
                    "ctl00$ContentPlaceHolder1$txtdname": "",
                    "ctl00$ContentPlaceHolder1$BtSelect": "查 询",
                    "ctl00$ContentPlaceHolder1$GridView2$ctl01$CheckBox1": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl02$CheckName": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl03$CheckName": "on",
                }
                CheckName_value = []
                for i in range(len(CheckName_name)):
                    CheckName_value.append('no')
                self.CheckName = dict(zip(CheckName_name, CheckName_value))
                datas = dict(datas, **self.CheckName)

                data_html = self.sess_guokongwencheng.post(url='http://data.wzshengwu.com/SaleQuery.aspx', data=datas,
                                                           headers=self.headers,
                                                           verify=False)
                # print('data_html.content.decode', data_html.content.decode('utf-8'))
                data_resps = etree.HTML(data_html.content.decode('utf-8'))
                data_len = int(len(data_resps.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr'))) - 1
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
                            '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[6]/nobr/text()' % (i + 2))[
                            0].strip()
                        if not drug_name:
                            drug_name = 1
                        # print('drug_name', drug_name)
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[7]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[8]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[9]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 销售类型
                            bill_types = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[2]/nobr/text()' % (i + 2))[
                                0].strip()
                            # print('bill_types', bill_types)

                            if '销售差价' in bill_types:
                                bill_type = 5
                            elif '销售退货' in bill_types:
                                bill_type = 4
                            else:
                                bill_type = 3

                        except:
                            bill_type = 3

                        try:
                            # 出库数量
                            drug_number = int(
                                data_resps.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[10]/nobr/text()' % (
                                            i + 2))[
                                    0].strip())

                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = re.findall(r'批号:(\d+)', data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (i + 2))[
                                0].strip().split(' ')[0])[0]
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = re.findall(r'效期:(\d{4}-\d{2}-\d{2})', data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (i + 2))[
                                0].strip().split(' ')[1])[0]
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            hospital_name = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[4]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[13]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[1]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_guokongwencheng'

                        try:
                            # 单价
                            drug_price = data_resps.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[12]/nobr/text()' % (i + 2))[
                                0].strip()
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

                        sql_crm = "insert into order_metadata_guokongwencheng(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
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

                        self.cursor.execute('select max(id) from order_metadata_guokongwencheng')
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

                # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                data_get_jin = self.sess_guokongwencheng.get(url='http://data.wzshengwu.com/PurList.aspx?type=00',
                                                             headers=self.headers,
                                                             verify=False)
                data_get_jin = etree.HTML(data_get_jin.content.decode('utf-8'))
                self.__VIEWSTATE_jin = data_get_jin.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                # print('__VIEWSTATE:', self.__VIEWSTATE_jin)

                datas_jin = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": self.__VIEWSTATE_jin,
                    "__VIEWSTATEGENERATOR": "4E664FDD",
                    "__SCROLLPOSITIONX": "0",
                    "__SCROLLPOSITIONY": "0",
                    "__VIEWSTATEENCRYPTED": "",
                    # "ctl00$ContentPlaceHolder1$tbdatebegin": "2018-07-02",
                    "ctl00$ContentPlaceHolder1$tbdatebegin": self.fist,
                    # "ctl00$ContentPlaceHolder1$tbdateend": "2018-10-29",
                    "ctl00$ContentPlaceHolder1$tbdateend": self.last,
                    "ctl00$ContentPlaceHolder1$BtSelect": "查 询",
                    "ctl00$ContentPlaceHolder1$GridView2$ctl01$CheckBox1": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl02$CheckName": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl03$CheckName": "on",
                }
                datas_jin = dict(datas_jin, **self.CheckName)
                data_html_jin = self.sess_guokongwencheng.post(url='http://data.wzshengwu.com/PurList.aspx?type=00',
                                                               data=datas_jin,
                                                               headers=self.headers,
                                                               verify=False)
                # print('data_html.content.decode', data_html_jin.content.decode('utf-8'))
                try:
                    page_num = int(re.findall(r'第1页/共(\d+)页', data_html_jin.content.decode('utf-8'))[0]) - 1
                except:
                    page_num = 0
                # print('page_num', page_num)
                data_resps_jin = etree.HTML(data_html_jin.content.decode('utf-8'))
                self.__VIEWSTATE_jin = data_resps_jin.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                data_len_jin = int(len(data_resps_jin.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr'))) - 1
                # print(data_len_jin)
                md5 = hashlib.md5()
                for i in range(data_len_jin):
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
                        drug_name = data_resps_jin.xpath(
                            '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                            0].strip().split(
                            '／')[0]
                        if not drug_name:
                            drug_name = 1

                        # print('drug_name', drug_name)
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                                0].strip().split('／')[1]
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[3]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[7]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = int(
                                data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[8]/nobr/text()' % (
                                            i + 2))[
                                    0].strip())

                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[9]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (i + 2))[
                                0].strip()
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
                            sell_time = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[2]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_guokongwencheng'

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

                        sql_crm = "insert into order_metadata_guokongwencheng(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
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

                        self.cursor.execute('select max(id) from order_metadata_guokongwencheng')
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

                for j in range(page_num):
                    datas_jin = {
                        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$GridView1$ctl18$lbnNext",
                        "__EVENTARGUMENT": "",
                        "__VIEWSTATE": self.__VIEWSTATE_jin,
                        "__VIEWSTATEGENERATOR": "4E664FDD",
                        "__SCROLLPOSITIONX": "0",
                        "__SCROLLPOSITIONY": "85",
                        "__VIEWSTATEENCRYPTED": "",
                        # "ctl00$ContentPlaceHolder1$tbdatebegin": "2017-09-01",
                        "ctl00$ContentPlaceHolder1$tbdatebegin": self.fist,
                        # "ctl00$ContentPlaceHolder1$tbdateend": "2018-10-31",
                        "ctl00$ContentPlaceHolder1$tbdateend": self.last,
                        "ctl00$ContentPlaceHolder1$GridView2$ctl01$CheckBox1": "on",
                        # "ctl00$ContentPlaceHolder1$GridView2$ctl02$CheckName": "on",
                        # "ctl00$ContentPlaceHolder1$GridView2$ctl03$CheckName": "on",
                    }
                    datas_jin = dict(datas_jin, **self.CheckName)
                    data_html_jin = self.sess_guokongwencheng.post(url='http://data.wzshengwu.com/PurList.aspx?type=00',
                                                                   data=datas_jin,
                                                                   headers=self.headers,
                                                                   verify=False)
                    # print('data_html.content.decode', data_html_jin.content.decode('utf-8'))
                    data_resps_jin = etree.HTML(data_html_jin.content.decode('utf-8'))
                    self.__VIEWSTATE_jin = data_resps_jin.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                    data_len_jin = int(
                        len(data_resps_jin.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr'))) - 1
                    # print(data_len_jin)
                    md5 = hashlib.md5()
                    for i in range(data_len_jin):
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
                            drug_name = data_resps_jin.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                                0].strip().split('／')[0]
                            if not drug_name:
                                drug_name = 1

                            # print('drug_name', drug_name)
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (
                                            i + 2))[
                                    0].strip().split('／')[1]
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[3]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[7]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                drug_unit = ''

                            try:
                                # 出库数量
                                drug_number = int(
                                    data_resps_jin.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[8]/nobr/text()' % (
                                                i + 2))[
                                        0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[9]/nobr/text()' % (
                                            i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (
                                            i + 2))[
                                    0].strip()
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
                                sell_time = data_resps_jin.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[2]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_guokongwencheng'

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

                            sql_crm = "insert into order_metadata_guokongwencheng(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
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

                            self.cursor.execute('select max(id) from order_metadata_guokongwencheng')
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

                # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                data_get_tui = self.sess_guokongwencheng.get(url='http://data.wzshengwu.com/PurList.aspx?type=00',
                                                             headers=self.headers,
                                                             verify=False)
                data_get_tui = etree.HTML(data_get_tui.content.decode('utf-8'))
                self.__VIEWSTATE_tui = data_get_tui.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                # print('__VIEWSTATE:', self.__VIEWSTATE_jin)

                datas_tui = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": self.__VIEWSTATE_tui,
                    "__VIEWSTATEGENERATOR": "4E664FDD",
                    "__SCROLLPOSITIONX": "0",
                    "__SCROLLPOSITIONY": "0",
                    "__VIEWSTATEENCRYPTED": "",
                    # "ctl00$ContentPlaceHolder1$tbdatebegin": "2017-09-01",
                    "ctl00$ContentPlaceHolder1$tbdatebegin": self.fist,
                    # "ctl00$ContentPlaceHolder1$tbdateend": "2018-10-31",
                    "ctl00$ContentPlaceHolder1$tbdateend": self.last,
                    "ctl00$ContentPlaceHolder1$BtSelect": "查 询",
                    "ctl00$ContentPlaceHolder1$GridView2$ctl01$CheckBox1": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl02$CheckName": "on",
                    # "ctl00$ContentPlaceHolder1$GridView2$ctl03$CheckName": "on",
                }
                datas_tui = dict(datas_tui, **self.CheckName)
                data_html_tui = self.sess_guokongwencheng.post(url='http://data.wzshengwu.com/PurList.aspx?type=10',
                                                               data=datas_tui,
                                                               headers=self.headers,
                                                               verify=False)
                # print('data_html.content.decode', data_html_tui.content.decode('utf-8'))
                try:
                    page_num = int(re.findall(r'第1页/共(\d+)页', data_html_tui.content.decode('utf-8'))[0]) - 1
                except:
                    page_num = 0

                # print('page_num', page_num)
                data_resps_tui = etree.HTML(data_html_tui.content.decode('utf-8'))
                self.__VIEWSTATE_tui = data_resps_tui.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                data_len_tui = int(len(data_resps_tui.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr'))) - 1
                # print(data_len_jin)
                md5 = hashlib.md5()
                for i in range(data_len_tui):
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
                    bill_type = 2
                    try:
                        drug_name = data_resps_tui.xpath(
                            '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                            0].strip().split(
                            '／')[0]
                        if not drug_name:
                            drug_name = 1

                        # print('drug_name', drug_name)
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                                0].strip().split('／')[1]
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[3]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[7]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = int(
                                data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[8]/nobr/text()' % (
                                            i + 2))[
                                    0].strip())

                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[9]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (i + 2))[
                                0]
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
                            sell_time = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[2]/nobr/text()' % (i + 2))[
                                0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_guokongwencheng'

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

                        sql_crm = "insert into order_metadata_guokongwencheng(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
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

                        self.cursor.execute('select max(id) from order_metadata_guokongwencheng')
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

                for j in range(page_num):
                    # print('self.__VIEWSTATE_tui', self.__VIEWSTATE_tui)
                    datas_tui = {
                        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$GridView1$ctl18$lbnNext",
                        "__EVENTARGUMENT": "",
                        "__VIEWSTATE": self.__VIEWSTATE_tui,
                        "__VIEWSTATEGENERATOR": "4E664FDD",
                        "__SCROLLPOSITIONX": "0",
                        "__SCROLLPOSITIONY": "102",
                        "__VIEWSTATEENCRYPTED": "",
                        # "ctl00$ContentPlaceHolder1$tbdatebegin": "2017-09-01",
                        "ctl00$ContentPlaceHolder1$tbdatebegin": self.fist,
                        # "ctl00$ContentPlaceHolder1$tbdateend": "2018-10-31",
                        "ctl00$ContentPlaceHolder1$tbdateend": self.last,
                        "ctl00$ContentPlaceHolder1$GridView2$ctl01$CheckBox1": "on",
                        # "ctl00$ContentPlaceHolder1$GridView2$ctl02$CheckName": "on",
                        # "ctl00$ContentPlaceHolder1$GridView2$ctl03$CheckName": "on",
                    }
                    datas_tui = dict(datas_tui, **self.CheckName)
                    data_html_tui = self.sess_guokongwencheng.post(url='http://data.wzshengwu.com/PurList.aspx?type=10',
                                                                   data=datas_tui,
                                                                   headers=self.headers,
                                                                   verify=False)
                    # print('data_html.content.decode', data_html_tui.content.decode('utf-8'))
                    data_resps_tui = etree.HTML(data_html_tui.content.decode('utf-8'))
                    self.__VIEWSTATE_tui = data_resps_tui.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                    data_len_tui = int(
                        len(data_resps_tui.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr'))) - 1
                    # print(data_len_tui)
                    md5 = hashlib.md5()
                    for i in range(data_len_tui):
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
                        bill_type = 2
                        try:
                            drug_name = data_resps_tui.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (i + 2))[
                                0].strip().split('／')[0]
                            if not drug_name:
                                drug_name = 1

                            # print('drug_name', drug_name)
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[5]/nobr/text()' % (
                                            i + 2))[
                                    0].strip().split('／')[1]
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[3]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[7]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                drug_unit = ''

                            try:
                                # 出库数量
                                drug_number = int(
                                    data_resps_tui.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[8]/nobr/text()' % (
                                                i + 2))[
                                        0].strip())

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[9]/nobr/text()' % (
                                            i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[11]/nobr/text()' % (
                                            i + 2))[
                                    0].strip()
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
                                sell_time = data_resps_tui.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[%s]/td[2]/nobr/text()' % (
                                            i + 2))[0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_guokongwencheng'

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

                            sql_crm = "insert into order_metadata_guokongwencheng(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
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

                            self.cursor.execute('select max(id) from order_metadata_guokongwencheng')
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
                        "SELECT count(*) from order_metadata_guokongwencheng WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_guokongwencheng WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '22-guokongwencheng',
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
                print('guokongwencheng-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_guokongwencheng WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_guokongwencheng WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '22-guokongwencheng',
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
