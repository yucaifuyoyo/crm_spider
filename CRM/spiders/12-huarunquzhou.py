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
import random
import json
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE


class HuarunquzhouSpider(scrapy.Spider):
    name = 'huarunquzhou'
    allowed_domains = ['.cn']
    start_urls = ['http://www.qzyy.net.cn:8080/syslogin.aspx?result=5&txtCompanyID=1']
    headers = HEADERS
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    sess_huarunquzhou = requests.session()
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
    __VIEWSTATEs = ''
    __EVENTVALIDATIONs = ''
    NavigationCorrector = ''

    def parse(self, response):
        self.number += 1
        # delivery_id = 'F617B115D6F3447983E94BB781231237'
        delivery_id = 'DDA100100M'
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
            sell_time = 0
            html = self.sess_huarunquzhou.get(url=self.start_urls[0], headers=self.headers, verify=False)
            resp = etree.HTML(html.content.decode('utf-8'))
            __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            image = self.sess_huarunquzhou.get(
                url="http://www.qzyy.net.cn:8080/verifyimage.aspx?time=%s" % (random.random()),
                headers=self.headers,
                verify=False)
            # print('image', image.url)
            # print('dict_from_cookiejar(image.cookies)', dict_from_cookiejar(image.cookies))
            if SCRAPYD_TYPE == 1:
                if 'indow' in platform.system():
                    symbol = r'\\'
                else:
                    symbol = r'/'
                path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                # print('path', path)
                files = r'{}{}static{}12-huarunquzhou'.format(path, symbol, symbol)
                if not os.path.exists(files):
                    os.makedirs(files)
                with open(r'{}{}static{}12-huarunquzhou{}yzm.jpg'.format(path, symbol, symbol, symbol), 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'{}{}static{}12-huarunquzhou{}yzm.jpg'.format(path, symbol, symbol, symbol)
            else:
                with open(r'./12-huarunquzhouyzm.jpg', 'wb') as f:
                    f.write(image.content)
                # 图片文件
                filename = r'./12-huarunquzhouyzm.jpg'

            codetype = 4004
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
                "UserName": get_account,
                "PassWord": get_pwd,
                "vcode": yzm,
            }
            self.sess_huarunquzhou.post(url="http://www.qzyy.net.cn:8080/syslogin.aspx?result=5&txtCompanyID=1",
                                        data=data, headers=self.headers, verify=False)
            psot_data_html = self.sess_huarunquzhou.get(
                url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?/BNumberTrafficFlowQuery&rs:Command=Render&rc:Parameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate={}&EndDate={}&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName='.format(
                    self.fist, self.last),
                # url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?/BNumberTrafficFlowQuery&rs:Command=Render&rc:Parameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate=2017-10-01&EndDate=2018-10-20&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName=',
                headers=self.headers,
                verify=False)
            # print('psot_data_html', psot_data_html.content.decode('utf-8'))
            # print('*' * 1000)
            psot_data_html = etree.HTML(psot_data_html.content.decode('utf-8'))
            __VIEWSTATE = psot_data_html.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __EVENTVALIDATION = psot_data_html.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
            data_data = {
                "AjaxScriptManager": "AjaxScriptManager|ReportViewerControl$ctl09$Reserved_AsyncLoadTarget",
                "__EVENTTARGET": "ReportViewerControl$ctl09$Reserved_AsyncLoadTarget",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": __VIEWSTATE,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "NavigationCorrector$ScrollPosition": "",
                "NavigationCorrector$ViewState": "",
                "NavigationCorrector$PageState": "",
                "NavigationCorrector$NewViewState": "",
                "ReportViewerControl$ctl03$ctl00": "",
                "ReportViewerControl$ctl03$ctl01": "",
                "ReportViewerControl$ctl10": "ltr",
                "ReportViewerControl$ctl11": "quirks",
                "ReportViewerControl$AsyncWait$HiddenCancelField": "False",
                "ReportViewerControl$ctl04$ctl03$txtValue": "NetSrv_App1",
                "ReportViewerControl$ctl04$ctl05$txtValue": "192.168.18.1",
                "ReportViewerControl$ctl04$ctl07$txtValue": "HYg8AE7GMeP2W2YGWaIEpg==",
                "ReportViewerControl$ctl04$ctl09$txtValue": "mztZ8O0gn1HUBnbz9wW68Q==",
                "ReportViewerControl$ctl04$ctl11$txtValue": "1",
                "ReportViewerControl$ctl04$ctl13$txtValue": "CUSR201703151777",
                "ReportViewerControl$ctl04$ctl15$txtValue": "",
                "ReportViewerControl$ctl04$ctl17$txtValue": "",
                # "ReportViewerControl$ctl04$ctl19$txtValue": "2017-10-01",
                "ReportViewerControl$ctl04$ctl19$txtValue": self.fist,
                "ReportViewerControl$ctl04$ctl21$txtValue": self.last,
                # "ReportViewerControl$ctl04$ctl21$txtValue": "2018-10-20",
                "ReportViewerControl$ctl04$ctl23$txtValue": "",
                "ReportViewerControl$ctl04$ctl25$txtValue": "",
                "ReportViewerControl$ctl04$ctl27$txtValue": "",
                "ReportViewerControl$ToggleParam$store": "",
                "ReportViewerControl$ToggleParam$collapse": "true",
                "ReportViewerControl$ctl05$ctl00$CurrentPage": "",
                "ReportViewerControl$ctl05$ctl03$ctl00": "",
                "ReportViewerControl$ctl08$ClientClickedId": "",
                "ReportViewerControl$ctl07$store": "",
                "ReportViewerControl$ctl07$collapse": "false",
                "ReportViewerControl$ctl09$VisibilityState$ctl00": "None",
                "ReportViewerControl$ctl09$ScrollPosition": "",
                "ReportViewerControl$ctl09$ReportControl$ctl02": "",
                "ReportViewerControl$ctl09$ReportControl$ctl03": "",
                "ReportViewerControl$ctl09$ReportControl$ctl04": "100",
                "__ASYNCPOST": "true",
            }
            # print('data_data', data_data)
            data_html = self.sess_huarunquzhou.post(
                url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?%2fBNumberTrafficFlowQuery&rs%3aCommand=Render&rc%3aParameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate={}&EndDate={}&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName='.format(
                    self.fist, self.last),
                # url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?%2fBNumberTrafficFlowQuery&rs%3aCommand=Render&rc%3aParameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate=2017-10-01&EndDate=2018-10-20&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName=',
                data=data_data, headers=self.headers,
                verify=False)
            # print('data_html', data_html.content.decode('utf-8'))
            # print('*' * 1000)
            self.__VIEWSTATEs = re.findall(r'__VIEWSTATE\|(.+?)\|', data_html.content.decode('utf-8'))[0]
            self.__EVENTVALIDATIONs = re.findall(r'__EVENTVALIDATION\|(.+?)\|', data_html.content.decode('utf-8'))[0]
            data_html = etree.HTML(data_html.content.decode('utf-8'))
            # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            try:
                self.NavigationCorrector = data_html.xpath('//input[@id="NavigationCorrector_NewViewState"]/@value')[0]
                data_len = int(len(data_html.xpath('//tr[@valign="top"]'))) - 1
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
                    # 表的名称
                    table_name = 'order_metadata_huarunquzhou'
                    try:
                        drug_name = data_html.xpath(
                            '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (i + 3))[
                            0].strip().split(
                            ' ')[
                            0]
                        if not drug_name:
                            drug_name = 0
                    except:
                        drug_name = 0

                    if drug_name != 0:
                        try:
                            # 金额
                            drug_price_sum = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[8]/div/text()' % (i + 3))[
                                0].strip()
                        except Exception as e:
                            drug_price_sum = ''
                            # print('drug_price_sum e:', e)

                        try:
                            # 药品规格
                            drug_specification = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (i + 3))[
                                0].strip().split(
                                ' ')[1]
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (i + 3))[
                                0].strip().split(
                                ' ')[2]
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[6]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 部门
                            department = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[12]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            department = ''

                        try:
                            # 类型
                            bill_types = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[2]/div/text()' % (i + 3))[
                                0].strip()

                            if bill_types == '进货':
                                bill_type = 1
                            elif bill_types == '进货退出':
                                bill_type = 2
                            elif bill_types == '销售退回':
                                bill_type = 4
                            else:
                                bill_type = 3
                        except:
                            bill_type = 3

                        try:
                            # 出库数量
                            drug_number = round(float(data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[5]/div/text()' % (i + 3))[
                                                          0].strip()))

                        except:
                            drug_number = 0

                        try:
                            # 批号
                            drug_batch = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[4]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            drug_batch = ''

                        try:
                            # 有效期至
                            valid_till = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[10]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            valid_till = '2000-01-01'

                        try:
                            # 医院(终端)名称
                            if bill_type == 1 or bill_type == 2:
                                hospital_name = ''
                            else:
                                hospital_name = data_html.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[3]/div/text()' % (
                                                i + 3))[
                                    0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            if bill_type == 1 or bill_type == 2:
                                hospital_address = ''
                            else:
                                hospital_address = data_html.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[13]/div/text()' % (
                                                i + 3))[
                                    0].strip()
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[1]/div/text()' % (i + 3))[
                                0].strip()

                            if sell_time == '汇  总':
                                sell_time = 0
                        except:
                            sell_time = '2000-01-01'

                        try:
                            # 价格
                            drug_price = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[7]/div/text()' % (i + 3))[
                                0].strip()
                            # '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[8]/div/text()' % (i + 3))[0]
                            # print('*'*1000)
                            # print('drug_price_sum', drug_price_sum)
                            # print('drug_price', drug_price)
                            # print('*'*1000)
                        except Exception as e:
                            drug_price = ''
                            # print('drug_price e:', e)

                        try:
                            # 业务编号
                            business_number = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[10]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            business_number = ''

                        try:
                            # 客户所属地区
                            customer_area = data_html.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[14]/div/text()' % (i + 3))[
                                0].strip()
                        except:
                            customer_area = ''

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                        update_time = create_time

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

                        sql_crm = "insert into order_metadata_huarunquzhou(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_price_sum, department, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, drug_price, sell_time, create_time, update_time, business_number, customer_area, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_price_sum, department,
                                                      drug_specification, supplier_name, drug_unit,
                                                      abs(drug_number), drug_batch, valid_till, hospital_name,
                                                      hospital_address, drug_price, sell_time, create_time, update_time,
                                                      business_number,
                                                      customer_area,
                                                      drug_hash, hospital_hash, stream_hash, month)
                        # print('sql_data', sql_data_crm)
                        try:
                            self.db.ping()
                        except pymysql.MySQLError:
                            self.db.connect()

                        try:
                            if sell_time != 0:
                                self.cursor.execute(sql_data_crm)
                                self.db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                        self.cursor.execute('select max(id) from order_metadata_huarunquzhou')
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
                            if sell_time != 0:
                                self.cursor.execute(sql_data_crm_data)
                                self.db.commit()
                                self.crm_cursor.execute(sql_data_crm_data)
                                self.crm_db.commit()
                        except Exception as e:
                            print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                self.NavigationCorrector = data_html.xpath('//input[@id="NavigationCorrector_NewViewState"]/@value')[0]
                for i in range(2, 1000):
                    data_datas = {
                        "AjaxScriptManager": "AjaxScriptManager|ReportViewerControl$ctl05$ctl00$CurrentPage",
                        # "AjaxScriptManager": "AjaxScriptManager|ReportViewerControl$ctl05$ctl00$Next$ctl00",
                        "NavigationCorrector$ScrollPosition": "",
                        "NavigationCorrector$ViewState": "",
                        "NavigationCorrector$PageState": "",
                        "NavigationCorrector$NewViewState": self.NavigationCorrector,
                        "ReportViewerControl$ctl03$ctl00": "",
                        "ReportViewerControl$ctl03$ctl01": "",
                        "ReportViewerControl$ctl10": "ltr",
                        "ReportViewerControl$ctl11": "quirks",
                        "ReportViewerControl$AsyncWait$HiddenCancelField": "False",
                        "ReportViewerControl$ctl04$ctl03$txtValue": "NetSrv_App1",
                        "ReportViewerControl$ctl04$ctl05$txtValue": "192.168.18.1",
                        "ReportViewerControl$ctl04$ctl07$txtValue": "HYg8AE7GMeP2W2YGWaIEpg==",
                        "ReportViewerControl$ctl04$ctl09$txtValue": "mztZ8O0gn1HUBnbz9wW68Q==",
                        "ReportViewerControl$ctl04$ctl11$txtValue": "1",
                        "ReportViewerControl$ctl04$ctl13$txtValue": "CUSR201703151777",
                        "ReportViewerControl$ctl04$ctl15$txtValue": "",
                        "ReportViewerControl$ctl04$ctl17$txtValue": "",
                        "ReportViewerControl$ctl04$ctl19$txtValue": self.fist,
                        # "ReportViewerControl$ctl04$ctl19$txtValue": "2017-10-01",
                        "ReportViewerControl$ctl04$ctl21$txtValue": self.last,
                        # "ReportViewerControl$ctl04$ctl21$txtValue": "2018-10-20",
                        "ReportViewerControl$ctl04$ctl23$txtValue": "",
                        "ReportViewerControl$ctl04$ctl25$txtValue": "",
                        "ReportViewerControl$ctl04$ctl27$txtValue": "",
                        "ReportViewerControl$ToggleParam$store": "",
                        "ReportViewerControl$ToggleParam$collapse": "true",
                        "ReportViewerControl$ctl05$ctl00$CurrentPage": i,
                        "ReportViewerControl$ctl05$ctl03$ctl00": "",
                        "ReportViewerControl$ctl08$ClientClickedId": "",
                        "ReportViewerControl$ctl07$store": "",
                        "ReportViewerControl$ctl07$collapse": "false",
                        "ReportViewerControl$ctl09$VisibilityState$ctl00": "ReportPage",
                        "ReportViewerControl$ctl09$ScrollPosition": "",
                        "ReportViewerControl$ctl09$ReportControl$ctl02": "",
                        "ReportViewerControl$ctl09$ReportControl$ctl03": "",
                        "ReportViewerControl$ctl09$ReportControl$ctl04": "100",
                        "__EVENTTARGET": "ReportViewerControl$ctl05$ctl00$CurrentPage",
                        # "__EVENTTARGET": "ReportViewerControl$ctl05$ctl00$Next$ctl00",
                        "__EVENTARGUMENT": "",
                        "__VIEWSTATE": self.__VIEWSTATEs,
                        "__EVENTVALIDATION": self.__EVENTVALIDATIONs,
                        "__ASYNCPOST": "true",
                    }
                    # print('data_datas', data_datas)
                    data_htmls = self.sess_huarunquzhou.post(
                        url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?%2fBNumberTrafficFlowQuery&rs%3aCommand=Render&rc%3aParameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate={}&EndDate={}&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName='.format(
                            self.fist, self.last),
                        # url='http://app1.yy5u.com:8080/ReportServer/Pages/ReportViewer.aspx?%2fBNumberTrafficFlowQuery&rs%3aCommand=Render&rc%3aParameters=false&SysCompanyID=1&UserID=CUSR201703151777&BizType=&GoodsID=&BeginDate=2017-10-01&EndDate=2018-10-20&serverNames=192.168.18.1&sqlName=NetSrv_App1&userName=mztZ8O0gn1HUBnbz9wW68Q%3d%3d&pass=HYg8AE7GMeP2W2YGWaIEpg%3d%3d&CustomerName=&BatchNo=&DepartName=',
                        data=data_datas, headers=self.headers,
                        verify=False)
                    self.__VIEWSTATEs = re.findall(r'__VIEWSTATE\|(.+?)\|', data_htmls.content.decode('utf-8'))[0]
                    self.__EVENTVALIDATIONs = \
                        re.findall(r'__EVENTVALIDATION\|(.+?)\|', data_htmls.content.decode('utf-8'))[0]
                    # print('-' * 1000)
                    # print("data_htmls.content.decode('utf-8')", data_htmls.content.decode('utf-8'))
                    # print('-' * 1000)
                    # try:
                    data_htmls = etree.HTML(data_htmls.content.decode('utf-8'))
                    self.NavigationCorrector = \
                        data_htmls.xpath('//input[@id="NavigationCorrector_NewViewState"]/@value')[0]
                    data_len = int(len(data_htmls.xpath('//tr[@valign="top"]'))) - 1
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
                        bill_type = 1
                        # 表的名称
                        table_name = 'order_metadata_huarunquzhou'
                        try:
                            drug_name = data_htmls.xpath(
                                '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (i + 3))[
                                0].strip().split(
                                ' ')[
                                0]
                            if not drug_name:
                                drug_name = 0
                        except:
                            drug_name = 0

                        if drug_name != 0:

                            try:
                                # 金额
                                drug_price_sum = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[8]/div/text()' % (
                                            i + 3))[0].strip()
                            except Exception as e:
                                drug_price_sum = ''
                                # print('drug_price_sum e:', e)

                            try:
                                # 药品规格
                                drug_specification = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (
                                            i + 3))[0].strip().split(
                                    ' ')[1]
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[9]/div/text()' % (
                                            i + 3))[0].strip().split(
                                    ' ')[2]
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[6]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                drug_unit = ''

                            try:
                                # 部门
                                department = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[12]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                department = ''

                            try:
                                # 类型
                                bill_types = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[2]/div/text()' % (
                                            i + 3))[0].strip()

                                if bill_types == '进货':
                                    bill_type = 1
                                elif bill_types == '进货退出':
                                    bill_type = 2
                                elif bill_types == '销售退回':
                                    bill_type = 4
                                else:
                                    bill_type = 3
                            except:
                                bill_type = 3

                            try:
                                # 出库数量
                                drug_number = round(float(data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[5]/div/text()' % (
                                            i + 3))[0].strip()))

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[4]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[10]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 医院(终端)名称
                                if bill_type == 1 or bill_type == 2:
                                    hospital_name = ''
                                else:
                                    hospital_name = data_htmls.xpath(
                                        '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[3]/div/text()' % (
                                                i + 3))[0].strip()
                            except:
                                hospital_name = ''

                            try:
                                # 医院(终端)地址
                                if bill_type == 1 or bill_type == 2:
                                    hospital_address = ''
                                else:
                                    hospital_address = data_htmls.xpath(
                                        '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[13]/div/text()' % (
                                                i + 3))[0].strip()
                            except:
                                hospital_address = ''

                            try:
                                # 销售(制单)时间
                                sell_time = data_htmls.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[1]/div/text()' % (
                                            i + 3))[0].strip()

                                if sell_time == '汇  总':
                                    sell_time = 0
                            except:
                                sell_time = '2000-01-01'

                            try:
                                # 价格
                                drug_price = data_html.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[7]/div/text()' % (
                                            i + 3))[0].strip()
                                # '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[8]/div/text()' % (i + 3))[0]
                                # print('*'*1000)
                                # print('drug_price_sum', drug_price_sum)
                                # print('drug_price', drug_price)
                                # print('*'*1000)
                            except Exception as e:
                                drug_price = ''
                                # print('drug_price e:', e)

                            try:
                                # 业务编号
                                business_number = data_html.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[10]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                business_number = ''

                            try:
                                # 客户所属地区
                                customer_area = data_html.xpath(
                                    '//div[@dir="LTR"]/table/tr/td/table/tr/td/table/tr[%s]/td[14]/div/text()' % (
                                            i + 3))[0].strip()
                            except:
                                customer_area = ''

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                            update_time = create_time

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

                            sql_crm = "insert into order_metadata_huarunquzhou(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_price_sum, department, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, hospital_name, hospital_address, drug_price, sell_time, create_time, update_time, business_number, customer_area, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type, drug_name, drug_price_sum, department,
                                                          drug_specification, supplier_name, drug_unit,
                                                          abs(drug_number), drug_batch, valid_till, hospital_name,
                                                          hospital_address, drug_price, sell_time, create_time,
                                                          update_time, business_number,
                                                          customer_area,
                                                          drug_hash, hospital_hash, stream_hash, month)
                            # print('sql_data', sql_data_crm)
                            try:
                                self.db.ping()
                            except pymysql.MySQLError:
                                self.db.connect()

                            try:
                                if sell_time != 0:
                                    self.cursor.execute(sql_data_crm)
                                    self.db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm:%s' % (e, sql_data_crm))

                            self.cursor.execute('select max(id) from order_metadata_huarunquzhou')
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
                                if sell_time != 0:
                                    self.cursor.execute(sql_data_crm_data)
                                    self.db.commit()
                                    self.crm_cursor.execute(sql_data_crm_data)
                                    self.crm_db.commit()
                            except Exception as e:
                                print('插入失败:%s  sql_data_crm_data:%s' % (e, sql_data_crm_data))

                    if sell_time == 0:

                        try:
                            crm_request_data = {
                                'version': delivery_id + "-" + self.time_stamp,
                                'streamType': streamType,
                            }
                            html = requests.post(url=CRM_REQUEST_URL, data=crm_request_data, headers=self.headers,
                                                 verify=False)
                            self.classify_success = json.loads(html.content.decode('utf-8'))['success']
                        except:
                            print('爬虫调取后端接口错误')

                        get_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        get_date = int(time.strftime("%Y%m%d", time.localtime()))
                        get_status = 1
                        if MONTHS == 0:
                            self.cursor.execute(
                                "SELECT count(*) from order_metadata_huarunquzhou WHERE sell_time='{}' and delivery_name='{}'".format(
                                    self.yesterday, enterprise_name))
                        else:
                            month = int(str(self.fist).replace('-', '')[0: 6])
                            self.cursor.execute(
                                "SELECT count(*) from order_metadata_huarunquzhou WHERE month='{}' and delivery_name='{}'".format(
                                    month, enterprise_name))
                        data_num = self.cursor.fetchone()[0]
                        remark = ''
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time
                        sql_crm_record = SQL_CRM_RECORD
                        sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                                    get_account, '12-huarunquzhou',
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
                        break

            except Exception as e:
                print('huarunquzhou-登入失败:%s' % e)
                print('self.number', self.number)
                if self.number < 4:
                    self.parse('aa')
                else:
                    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    get_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    get_date = int(time.strftime("%Y%m%d", time.localtime()))
                    get_status = 2
                    if MONTHS == 0:
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_huarunquzhou WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_huarunquzhou WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '12-huarunquzhou',
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
