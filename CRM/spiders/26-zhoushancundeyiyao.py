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


class LongquanSpider(scrapy.Spider):
    name = 'zhoushancundeyiyao'
    # allowed_domains = ['.']
    start_urls = ['http://www.zscdyy.cn/cdyy/zh_index.asp']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    # fist = '2018-10-01'
    fist = FIST
    # last = '2018-11-06'
    last = LAST
    sess_zhoushancundeyiyao = requests.session()
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
        # delivery_id = 'F617B115D6F3447983E94BB781231289'
        delivery_id = 'DDA100101C'
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
            html = self.sess_zhoushancundeyiyao.get(url=self.start_urls[0], headers=self.headers, verify=False)
            resp = etree.HTML(html.content.decode('gb2312'))
            __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
            Login_data = {
                "__VIEWSTATE": __VIEWSTATE,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "tbLoadId": get_account,
                "tbLoadPwd": get_pwd,
                "bConfirm": "确 定".encode('gb2312'),
                "comeurl": "http://www.cdyy.com/list.asp?id=2435",
            }

            login_html = self.sess_zhoushancundeyiyao.post(url='http://www.zscdyy.cn/Default.aspx',
                                                           data=Login_data, headers=self.headers, verify=False)
            # print('login_html', login_html.content.decode('gb2312'))
            # print('Login_data', Login_data)
            psot_data_html = self.sess_zhoushancundeyiyao.get(url='http://www.zscdyy.cn/SupplyManage/SaleDetail2.aspx',
                                                              headers=self.headers, verify=False)
            try:
                re.findall(r'进销存查询', psot_data_html.content.decode('gb2312', 'igonre'))[0]
                # print('psot_data_html', psot_data_html.content.decode('gb2312'))
                # print('*' * 1000)
                psot_data_html = etree.HTML(psot_data_html.content.decode('gb2312'))
                chk_name = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[1]/@name')
                Txt_value = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[2]/@value')
                Txt_name = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[2]/@name')
                __VIEWSTATE = psot_data_html.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                __EVENTVALIDATION = psot_data_html.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                data_data = {
                    "__VIEWSTATE": __VIEWSTATE,
                    "__EVENTVALIDATION": __EVENTVALIDATION,
                    "txtStartDate": self.fist,
                    "txtEndDate": self.last,
                    "btnSearch": "查 询".encode('gb2312'),
                    "tbProductId": "",
                    "PName": "",
                    "Tsc$ctl01$CheckBoxAll": "on",
                    # "Tsc$ctl02$chk": "on",
                    # "Tsc$ctl02$Txt": "110331",
                    # "Tsc$ctl03$chk": "on",
                    # "Tsc$ctl03$Txt": "115231",
                    # "Tsc$ctl04$chk": "on",
                    # "Tsc$ctl04$Txt": "10356",
                    # "Tsc$ctl05$chk": "on",
                    # "Tsc$ctl05$Txt": "121434",
                    # "Tsc$ctl06$chk": "on",
                    # "Tsc$ctl06$Txt": "117675",
                    # "Tsc$ctl07$chk": "on",
                    # "Tsc$ctl07$Txt": "121913",
                    # "Tsc$ctl08$chk": "on",
                    # "Tsc$ctl08$Txt": "121622",
                    # "Tsc$ctl09$chk": "on",
                    # "Tsc$ctl09$Txt": "121758",
                    # "Tsc$ctl10$chk": "on",
                    # "Tsc$ctl10$Txt": "121422",
                    # "Tsc$ctl11$chk": "on",
                    # "Tsc$ctl11$Txt": "112845",
                    # "Tsc$ctl12$chk": "on",
                    # "Tsc$ctl12$Txt": "121792",
                    # "Tsc$ctl13$chk": "on",
                    # "Tsc$ctl13$Txt": "117525",
                    # "Tsc$ctl14$chk": "on",
                    # "Tsc$ctl14$Txt": "122742",
                }
                chk_value = []
                for i in range(len(chk_name)):
                    chk_value.append('no')
                chk = dict(zip(chk_name, chk_value))
                Txt = dict(zip(Txt_name, Txt_value))
                data_data = dict(data_data, **chk, **Txt)
                data_html = self.sess_zhoushancundeyiyao.post(url='http://www.zscdyy.cn/SupplyManage/SaleDetail2.aspx',
                                                              data=data_data, headers=self.headers,
                                                              verify=False)
                # print('data_html', data_html.content.decode('gb2312', 'ignore'))
                # print('data_data', data_data)
                # print('*' * 1000)
                data_html = etree.HTML(data_html.content.decode('gb2312', 'ignore'))
                # //*[@id="ContentPlaceHolder3_GridBlock76067"]/tbody/tr[1]
                data_len = int(len(data_html.xpath('//*[@id="Dgv"]/tr'))) - 2
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
                            '//*[@id="Dgv"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                        if not drug_name:
                            drug_name = 1
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[6]/text()' % (i + 2))[0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = ''
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = ''
                        except:
                            drug_unit = ''

                        try:
                            types = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                            # 单据类型:1进货,2退货,3销售,4销售退货
                            if '销售单' in types:
                                bill_type = 3
                            elif '销售退货单' in types:
                                bill_type = 4
                            else:
                                bill_type = 5

                        except:
                            bill_type = 5

                        try:
                            # 出库数量
                            drug_number = round(float(data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()))
                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[7]/text()' % (i + 2))[0].strip()
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
                                '//*[@id="Dgv"]/tr[%s]/td[5]/text()' % (i + 2))[0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = ''
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_times = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                            if '合计' in sell_times:
                                sell_time = 5
                            else:
                                sell_time = sell_times
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 价格
                            drug_price = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[9]/text()' % (i + 2))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_zhoushancundeyiyao'

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

                        sql_crm = "insert into order_metadata_zhoushancundeyiyao(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_hash, hospital_hash, stream_hash, drug_price, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, sell_time, create_time, update_time,
                                                      drug_hash, hospital_hash, stream_hash, drug_price, month)
                        # print('sql_data', sql_data_crm)
                        try:
                            self.db.ping()
                        except pymysql.MySQLError:
                            self.db.connect()

                        try:
                            if sell_time != 5:
                                self.cursor.execute(sql_data_crm)
                                self.db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                        self.cursor.execute('select max(id) from order_metadata_zhoushancundeyiyao')
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
                                if sell_time != 5:
                                    self.cursor.execute(sql_data_crm_data)
                                    self.db.commit()
                                    self.crm_cursor.execute(sql_data_crm_data)
                                    self.crm_db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                psot_data_html = self.sess_zhoushancundeyiyao.get(
                    url='http://www.zscdyy.cn/SupplyManage/TotalAccountInfoByProductQuery2.aspx',
                    headers=self.headers, verify=False)
                # print('psot_data_html', psot_data_html.content.decode('gb2312'))
                # print('*' * 1000)
                psot_data_html = etree.HTML(psot_data_html.content.decode('gb2312'))
                __VIEWSTATE = psot_data_html.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                __EVENTVALIDATION = psot_data_html.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                chk_name = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[1]/@name')
                Txt_value = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[2]/@value')
                Txt_name = psot_data_html.xpath('//*[@id="Tsc"]/tr/td/input[2]/@name')
                data_data = {
                    "__VIEWSTATE": __VIEWSTATE,
                    "__EVENTVALIDATION": __EVENTVALIDATION,
                    "txtStartDate": self.fist,
                    "txtEndDate": self.last,
                    "btnSearch": "查 询".encode('gb2312'),
                    "tbProductId3": "",
                    "txtClientName": "",
                    "tbProductId": "",
                    "PName": "",
                    "Tsc$ctl01$CheckBoxAll": "on",
                    # "Tsc$ctl02$chk": "on",
                    # "Tsc$ctl02$Txt": "110331",
                    # "Tsc$ctl03$chk": "on",
                    # "Tsc$ctl03$Txt": "115231",
                    # "Tsc$ctl04$chk": "on",
                    # "Tsc$ctl04$Txt": "10356",
                    # "Tsc$ctl05$chk": "on",
                    # "Tsc$ctl05$Txt": "121434",
                    # "Tsc$ctl06$chk": "on",
                    # "Tsc$ctl06$Txt": "117675",
                    # "Tsc$ctl07$chk": "on",
                    # "Tsc$ctl07$Txt": "121913",
                    # "Tsc$ctl08$chk": "on",
                    # "Tsc$ctl08$Txt": "121622",
                    # "Tsc$ctl09$chk": "on",
                    # "Tsc$ctl09$Txt": "121758",
                    # "Tsc$ctl10$chk": "on",
                    # "Tsc$ctl10$Txt": "121422",
                    # "Tsc$ctl11$chk": "on",
                    # "Tsc$ctl11$Txt": "112845",
                    # "Tsc$ctl12$chk": "on",
                    # "Tsc$ctl12$Txt": "121792",
                    # "Tsc$ctl13$chk": "on",
                    # "Tsc$ctl13$Txt": "117525",
                    # "Tsc$ctl14$chk": "on",
                    # "Tsc$ctl14$Txt": "122742",
                }
                chk_value = []
                for i in range(len(chk_name)):
                    chk_value.append('no')
                chk = dict(zip(chk_name, chk_value))
                Txt = dict(zip(Txt_name, Txt_value))
                data_data = dict(data_data, **chk, **Txt)
                data_html = self.sess_zhoushancundeyiyao.post(
                    url='http://www.zscdyy.cn/SupplyManage/TotalAccountInfoByProductQuery2.aspx',
                    data=data_data, headers=self.headers,
                    verify=False)
                # print('data_data', data_data)
                # print('*' * 1000)
                data_html = etree.HTML(data_html.content.decode('gb2312', 'ignore'))
                # //*[@id="ContentPlaceHolder3_GridBlock76067"]/tbody/tr[1]
                data_len = int(len(data_html.xpath('//*[@id="Dgv"]/tr'))) - 2
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
                        drug_name = data_html.xpath(
                            '//*[@id="Dgv"]/tr[%s]/td[3]/text()' % (i + 2))[0].strip()
                        if not drug_name:
                            drug_name = 1
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[4]/text()' % (i + 2))[0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[11]/text()' % (i + 2))[0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[5]/text()' % (i + 2))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            types = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[2]/text()' % (i + 2))[0].strip()
                            # 单据类型:1进货,2退货,3销售,4销售退货
                            if '入库单' in types:
                                bill_type = 1
                            elif '采购退货单' in types:
                                bill_type = 2
                            else:
                                bill_type = 5

                        except:
                            bill_type = 5

                        try:
                            # 出库数量
                            drug_number = round(float(data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[8]/text()' % (i + 2))[0].strip()))
                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[9]/text()' % (i + 2))[0].strip()
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
                            sell_times = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[6]/text()' % (i + 2))[0].strip()
                            if '合计' in sell_times:
                                sell_time = 5
                            else:
                                sell_time = sell_times
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 价格
                            drug_price = data_html.xpath(
                                '//*[@id="Dgv"]/tr[%s]/td[10]/text()' % (i + 2))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_zhoushancundeyiyao'

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

                        sql_crm = "insert into order_metadata_zhoushancundeyiyao(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_hash, hospital_hash, stream_hash, drug_price, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, sell_time, create_time, update_time,
                                                      drug_hash, hospital_hash, stream_hash, drug_price, month)
                        # print('sql_data', sql_data_crm)
                        try:
                            self.db.ping()
                        except pymysql.MySQLError:
                            self.db.connect()

                        try:
                            if sell_time != 5:
                                self.cursor.execute(sql_data_crm)
                                self.db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                        self.cursor.execute('select max(id) from order_metadata_zhoushancundeyiyao')
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
                                if sell_time != 5:
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
                create_time = get_time
                update_time = create_time
                get_date = int(time.strftime("%Y%m%d", time.localtime()))
                get_status = 1
                if MONTHS == 0:
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_zhoushancundeyiyao WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_zhoushancundeyiyao WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '37-zhoushancundeyiyao',
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
                print('zhoushancundeyiyao-登入失败:%s' % e)
                print('self.number', self.number)
                if self.number < 4:
                    self.parse('aa')
                else:
                    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    update_time = create_time
                    get_time = create_time
                    get_date = int(time.strftime("%Y%m%d", time.localtime()))
                    get_status = 2
                    if MONTHS == 0:
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_zhoushancundeyiyao WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_zhoushancundeyiyao WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '37-zhoushancundeyiyao',
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
