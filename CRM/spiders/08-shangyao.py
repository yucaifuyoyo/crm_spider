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


class ShangyaoSpider(scrapy.Spider):
    name = 'shangyao'
    allowed_domains = ['.com']
    start_urls = ['http://passport.shaphar.com/cas-webapp-server/login']
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    sess_shangyao = requests.session()
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
        # delivery_id = 'F617B115D6F3447983E94BB781231231'
        delivery_id = 'DDA1001009'
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
            html = self.sess_shangyao.get(url=self.start_urls[0], headers=self.headers, verify=False)
            resp = etree.HTML(html.content.decode('utf-8'))
            post_url = 'http://passport.shaphar.com/' + resp.xpath('//*[@id="form1"]/@action')[0]
            lt = resp.xpath('//*[@name="lt"]/@value')[0]
            image = self.sess_shangyao.get(url='http://passport.shaphar.com/cas-webapp-server/kaptcha.jpg',
                                           headers=self.headers,
                                           verify=False)
            # print('dict_from_cookiejar(image.cookies)', dict_from_cookiejar(image.cookies))
            if SCRAPYD_TYPE == 1:
                if 'indow' in platform.system():
                    symbol = r'\\'
                else:
                    symbol = r'/'
                path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                files = r'{}{}static{}08-shangyao'.format(path, symbol, symbol)
                if not os.path.exists(files):
                    os.makedirs(files)
                with open(r'{}{}static{}08-shangyao{}yzm.jpg'.format(path, symbol, symbol, symbol), 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'{}{}static{}08-shangyao{}yzm.jpg'.format(path, symbol, symbol, symbol)
            else:
                with open(r'./08-shangyaoyzm.jpg', 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'./08-shangyaoyzm.jpg'

            codetype = 1004
            # 超时时间，秒
            timeout = 60
            ydm = YDMHttp()
            cid, code_result = ydm.run(filename, codetype, timeout)
            # yzm = input('请输入验证码：')
            # print('cid:%s   code_result:%s' % (cid, code_result))
            yzm = code_result
            # yzm = input('请输入验证码:')
            data = {
                "username": get_account,
                "password": get_pwd,
                "captcha": yzm,
                "lt": lt,
                "_eventId": "submit",
                "submit": "登录",
            }
            self.sess_shangyao.post(url=post_url, data=data, headers=self.headers, verify=False)
            self.sess_shangyao.get(
                url='http://applyreport.shaphar.com/WebReport1/ReportServer?op=fs&portalname=FE8EC3D50BFD98BBC9C1D07E55C9E019',
                headers=self.headers, verify=False)
            # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            # '''
            try:
                js_html = self.sess_shangyao.get(
                    url='http://applyreport.shaphar.com/WebReport1/ReportServer?op=fs_main&cmd=entry_report&id=22078',
                    headers=self.headers, verify=False)
                sessionID = re.findall(r'sessionID=(.+?)"', js_html.content.decode('gbk'))[0]

                time_data = {
                    '__parameters__': '{"AS_CLIENT":"","ENDDATE":"%s","COM_GOODS":"","AS_SALE_TYPE":false,"TCXT":false,"INV_OWNER":"","LABEL0":"[5e93][5b58][62e5][6709][8005][ff1a]","AS_DATE_TYPE":"SEND","SALE_ORG":"","SORT5":"asc","COLUMN5":"","SORT4":"asc","COLUMN4":"","SORT3":"asc","COLUMN3":"","SORT2":"asc","COLUMN2":"","SORT1":"asc","COLUMN1":"","LABEL0_C_C_C_C_C":"[4ea7][54c1][ff1a]","LABEL1":"[6392][5e8f][ff1a]","LABEL0_C_C_C_C":"[5ba2][6237][ff1a]","LABEL0_C_C_C":"[9500][552e][90e8][95e8][ff1a]","UPDATE":"%s","STARTDATE":"%s"}' % (
                        self.last, self.last, self.fist),
                }
                time_url = 'http://applyreport.shaphar.com/WebReport1/ReportServer?op=fr_dialog&cmd=parameters_d&sessionID={}'.format(
                    sessionID)
                self.sess_shangyao.post(url=time_url, data=time_data, headers=self.headers, verify=False)
                time_time = int(time.time() * 1000)
                data_url = 'http://applyreport.shaphar.com/WebReport1/ReportServer?_={}&__boxModel__=true&op=fr_write&cmd=read_w_content&sessionID={}&reportIndex=0&browserWidth=1690&__cutpage__=v&pn=1'.format(
                    time_time, sessionID)
                data_htmls = self.sess_shangyao.get(url=data_url, headers=self.headers, verify=False)
                data_resps = etree.HTML(data_htmls.content.decode('gbk'))

                # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                js_html = self.sess_shangyao.get(
                    url='http://applyreport.shaphar.com/WebReport1/ReportServer?op=fs_main&cmd=entry_report&id=22079',
                    headers=self.headers, verify=False)
                sessionID = re.findall(r'sessionID=(.+?)"', js_html.content.decode('gbk'))[0]
                time_data_bian = {
                    '__parameters__': '{"AS_CLIENT":"","ENDDATE":"%s","AS_LOT":"","LABEL0_C_C_C_C_C_C_C_C":"[6279][53f7][ff1a]","AS_COM_GOODS":"","AS_SALE_TYPE":false,"TCXT":false,"AS_INV_OWNER":"","LABEL0":"[5e93][5b58][62e5][6709][8005][ff1a]","AS_DATE_TYPE":"SEND","AS_SALE_ORG":"","SORT5":"asc","COLUMN5":"","SORT4":"asc","COLUMN4":"","SORT3":"asc","COLUMN3":"","SORT2":"asc","COLUMN2":"","SORT1":"asc","COLUMN1":"","LABEL0_C_C_C_C_C":"[4ea7][54c1][ff1a]","LABEL1":"[6392][5e8f][ff1a]","LABEL0_C_C_C_C":"[5ba2][6237][ff1a]","LABEL0_C_C_C":"[9500][552e][90e8][95e8][ff1a]","UPDATE":"%s","STARTDATE":"%s"}' % (
                        self.last, self.last, self.fist)
                }
                time_url_bian = 'http://applyreport.shaphar.com/WebReport1/ReportServer?op=fr_dialog&cmd=parameters_d&sessionID={}'.format(
                    sessionID)
                self.sess_shangyao.post(url=time_url_bian, data=time_data_bian, headers=self.headers, verify=False)
                time_times = int(time.time() * 1000)
                data_url_bian = 'http://applyreport.shaphar.com/WebReport1/ReportServer?_={}&__boxModel__=true&op=fr_write&cmd=read_w_content&sessionID={}&reportIndex=0&browserWidth=1690&__cutpage__=v&pn=1'.format(
                    time_times, sessionID)
                data_htmls_bian = self.sess_shangyao.get(url=data_url_bian, headers=self.headers, verify=False)
                data_resps_bian = etree.HTML(data_htmls_bian.content.decode('gbk'))
                data_len = len(data_resps_bian.xpath(
                    '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr'))
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
                            '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                    i + 1))[0].strip().split('-')[0]
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                        i + 1))[0].strip().split('-')[1]
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                        i + 1))[0].strip().split('-')[2]
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[4]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = int(data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[8]/div/text()' % (
                                        i + 1))[0].strip())
                            if drug_number < 0:
                                bill_type = 4
                        except:
                            drug_number = 0

                        try:
                            # 订单数量
                            indent_number = int(data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[7]/div/text()' % (
                                        i + 1))[0].strip())
                        except:
                            indent_number = 0

                        try:
                            # 批号
                            drug_batch = data_resps_bian.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[8]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = data_resps_bian.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[9]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            hospital_name = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[6]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            hospital_address = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[11]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_resps.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[2]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 出库日期
                            out_put_time = data_resps.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            out_put_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_shangyao'

                        try:
                            # 单价
                            drug_price = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[9]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_price = ''
                        try:
                            # 单次总价
                            drug_price_sum = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[10]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_price_sum = ''

                        try:
                            # 商品id
                            goods_id = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[2]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            goods_id = ''

                        try:
                            # 订单号
                            order_number = data_resps.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[4]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            order_number = ''

                        try:
                            # 销售部门
                            sales_departments = data_resps.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[1]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            sales_departments = '西药销售部'

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

                        sql_crm = "insert into order_metadata_shangyao(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, indent_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, out_put_time, create_time, update_time, drug_price, drug_price_sum, goods_id, order_number, sales_departments, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), indent_number, drug_batch, valid_till,
                                                      hospital_name,
                                                      hospital_address, sell_time, out_put_time, create_time,
                                                      update_time, drug_price,
                                                      drug_price_sum, goods_id, order_number, sales_departments,
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

                        self.cursor.execute('select max(id) from order_metadata_shangyao')
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

                # '''
                # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                jin_html = self.sess_shangyao.get(
                    url='http://applyreport.shaphar.com/WebReport1/ReportServer?op=fs_main&cmd=entry_report&id=22077',
                    headers=self.headers, verify=False)
                sessionID = re.findall(r'sessionID=(.+?)"', jin_html.content.decode('gbk'))[0]
                time_data_jin = {
                    '__parameters__': '{"AS_COM_GOODS":"","ENDDATE":"%s","LABEL5":"[4ed3][5e93][ff1a]","LABEL3":"[5e93][5b58][62e5][6709][8005][ff1a]","LABEL2":"[4ea7][54c1][ff1a]","LABEL0":"[91c7][8d2d][65e5][671f][ff1a]","AS_INV_STORAGE":"","AS_INV_OWNER":"","SORT5":"asc","COLUMN5":"","SORT4":"asc","COLUMN4":"","SORT3":"asc","COLUMN3":"","SORT2":"asc","COLUMN2":"","SORT1":"asc","COLUMN1":"","LABEL1":"[6392][5e8f][ff1a]","LABEL0_C_C":"[2014]","UPDATE":"%s","STARTDATE":"%s"}' % (
                        self.last, self.last, self.fist)
                }
                time_url_bian = 'http://applyreport.shaphar.com/WebReport1/ReportServer?op=fr_dialog&cmd=parameters_d&sessionID={}'.format(
                    sessionID)
                self.sess_shangyao.post(url=time_url_bian, data=time_data_jin, headers=self.headers, verify=False)
                time_times = int(time.time() * 1000)
                data_url_jin = 'http://applyreport.shaphar.com/WebReport1/ReportServer?_={}&__boxModel__=true&op=fr_write&cmd=read_w_content&sessionID={}&reportIndex=0&browserWidth=1690&__cutpage__=v&pn=1'.format(
                    time_times, sessionID)
                data_htmls_jin = self.sess_shangyao.get(url=data_url_jin, headers=self.headers, verify=False)
                data_resps_jin = etree.HTML(data_htmls_jin.content.decode('gbk'))
                data_len = len(data_resps_jin.xpath(
                    '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr'))
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
                        drug_name = data_resps_jin.xpath(
                            '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[1]/div/text()' % (
                                    i + 1))[0].strip().split('-')[0]
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[1]/div/text()' % (
                                        i + 1))[0].strip().split('-')[1]
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[1]/div/text()' % (
                                        i + 1))[0].strip().split('-')[2]
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[2]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 出库数量
                            drug_number = int(data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                        i + 1))[0].strip())
                            if drug_number < 0:
                                bill_type = 2
                        except:
                            drug_number = 0

                        try:
                            # 订单数量
                            indent_number = int(0)
                        except:
                            indent_number = 0

                        try:
                            # 批号
                            drug_batch = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[4]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[5]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            valid_till = ''

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
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[2]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 出库日期
                            out_put_time = data_resps_jin.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[2]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            out_put_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_shangyao'

                        try:
                            # 单价
                            drug_price = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[6]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_price = ''
                        try:
                            # 单次总价
                            drug_price_sum = data_resps_jin.xpath(
                                '//div[@id="frozen-center"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[7]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            drug_price_sum = ''

                        try:
                            # 商品id
                            goods_id = data_resps_jin.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[5]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            goods_id = ''

                        try:
                            # 订单号
                            order_number = data_resps_jin.xpath(
                                '//div[@id="frozen-west"]/table[@class="x-table"]/tbody[@class="rows-height-counter"]/tr[%s]/td[3]/div/text()' % (
                                        i + 1))[0].strip()
                        except:
                            order_number = ''

                        try:
                            # 销售部门
                            sales_departments = ''
                        except:
                            sales_departments = ''

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

                        sql_crm = "insert into order_metadata_shangyao(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, indent_number, drug_batch, valid_till, hospital_name, hospital_address, sell_time, out_put_time, create_time, update_time, drug_price, drug_price_sum, goods_id, order_number, sales_departments, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), indent_number, drug_batch, valid_till,
                                                      hospital_name,
                                                      hospital_address, sell_time, out_put_time, create_time,
                                                      update_time, drug_price,
                                                      drug_price_sum, goods_id, order_number, sales_departments,
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

                        self.cursor.execute('select max(id) from order_metadata_shangyao')
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
                        "SELECT count(*) from order_metadata_shangyao WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_shangyao WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '08-shangyao',
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
                print('shangyao-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_shangyao WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_shangyao WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '08-shangyao',
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
