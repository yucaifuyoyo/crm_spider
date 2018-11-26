# -*- coding: utf-8 -*-
import re
import time
import random
import hashlib
import pymysql
import datetime
from lxml import etree
import scrapy
from scrapy import Request
from scrapy.selector import Selector
import requests
from requests.utils import dict_from_cookiejar

from CRM.items import EastpharmItem, VerItem
import datetime
import json
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE


class EastpharmSpider(scrapy.Spider):
    name = 'ningbohuadong'
    allowed_domains = ['eastpharm.com']
    start_urls = ['http://dzsw.eastpharm.com/web_login.aspx?&ReturnUrl=default.aspx']
    sess_ningbohuadong = requests.Session()
    headers = HEADERS
    time_stamp = str(int(time.time() * 1000))
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
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

    def parse(self, response):
        delivery_id = 'F617B115D6F3447983E94BB781231269'
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
            html = self.sess_ningbohuadong.get(url=self.start_urls[0], headers=self.headers, verify=False)
            resp = etree.HTML(html.content.decode('utf-8'))
            __VIEWSTATE = resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
            __EVENTVALIDATION = resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
            res = self.sess_ningbohuadong.get("http://lx.eastpharm.com/Confirmation.aspx", headers=self.headers,
                                              verify=False)
            txtcheck = dict_from_cookiejar(res.cookies).get("yzmcode")

            post_data = {
                "__VIEWSTATE": __VIEWSTATE,
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "txtusername": get_account,
                "txtpassword": get_pwd,
                "txtcheck": txtcheck,
                "btnlogin.x": str(random.randint(2, 40)),
                "btnlogin.y": str(random.randint(2, 26)),
            }
            # print('post_data', post_data)
            self.sess_ningbohuadong.post(url='http://lx.eastpharm.com/web_login.aspx?l=1&ReturnUrl=%2fdefault.aspx',
                                         headers=self.headers, data=post_data)
            data_get = self.sess_ningbohuadong.get(url='http://lx.eastpharm.com/Flow_Query.aspx', headers=self.headers,
                                                   verify=False)
            # print('data_get.content.decode', data_get.content.decode('utf-8'))
            # print('*' * 1000)
            try:
                data_get = etree.HTML(data_get.content.decode('utf-8'))
                self.__VIEWSTATEs = data_get.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                self.__EVENTVALIDATIONs = data_get.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                post_data = {
                    "ctl00$ToolkitScriptManager2": "ctl00$ContentPlaceHolder3$UpdatePanel1|ctl00$ContentPlaceHolder3$btnselect",
                    "ctl00_ToolkitScriptManager2_HiddenField": "",
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": self.__VIEWSTATEs,
                    "ctl00$ZBMENUPARA": "",
                    "ctl00$ContentPlaceHolder3$ddldt": "2",
                    # "ctl00$ContentPlaceHolder3$tb_begin": "2017-10-01",
                    "ctl00$ContentPlaceHolder3$tb_begin": self.fist,
                    "ctl00$ContentPlaceHolder3$ddltype": "0",
                    "ctl00$ContentPlaceHolder3$tb_cus": "",
                    "ctl00$ContentPlaceHolder3$ddlprov": "0",
                    "ctl00$ContentPlaceHolder3$tb_area": "",
                    # "ctl00$ContentPlaceHolder3$tb_end": "2018-10-30",
                    "ctl00$ContentPlaceHolder3$tb_end": self.last,
                    "ctl00$ContentPlaceHolder3$ddlgoods": "0",
                    "ctl00$ContentPlaceHolder3$tb_addr": "",
                    "ctl00$ContentPlaceHolder3$txtpagesize1": "15",
                    "ctl00$ContentPlaceHolder3$Text1": "",
                    "ctl00$ContentPlaceHolder3$txtpagesize": "15",
                    "ctl00$ContentPlaceHolder3$pageno": "",
                    "__VIEWSTATEENCRYPTED": "",
                    "__EVENTVALIDATION": self.__EVENTVALIDATIONs,
                    "ctl00$ContentPlaceHolder3$btnselect": "查询",
                }

                post_resp = self.sess_ningbohuadong.post(url='http://lx.eastpharm.com/Flow_Query.aspx',
                                                         headers=self.headers,
                                                         data=post_data)
                # print('post_resp', post_resp.content.decode('utf-8'))
                post_resp = etree.HTML(post_resp.content.decode('utf-8'))
                try:
                    page_num = int(post_resp.xpath('//*[@id="ctl00_ContentPlaceHolder3_lbl_totalP1"]/text()')[0]) + 1
                except:
                    page_num = 0
                # print('page_num', page_num)
                self.__VIEWSTATEs = post_resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                self.__EVENTVALIDATIONs = post_resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                for j in range(1, page_num):
                    post_data = {
                        "ctl00$ToolkitScriptManager2": "ctl00$ContentPlaceHolder3$UpdatePanel1|ctl00$ContentPlaceHolder3$Imagebutton5",
                        "ctl00_ToolkitScriptManager2_HiddenField": "",
                        "__EVENTTARGET": "",
                        "__EVENTARGUMENT": "",
                        "__LASTFOCUS": "",
                        "__VIEWSTATE": self.__VIEWSTATEs,
                        "ctl00$ZBMENUPARA": "",
                        "ctl00$ContentPlaceHolder3$ddldt": "2",
                        # "ctl00$ContentPlaceHolder3$tb_begin": "2016-10-01",
                        "ctl00$ContentPlaceHolder3$tb_begin": self.fist,
                        "ctl00$ContentPlaceHolder3$ddltype": "0",
                        "ctl00$ContentPlaceHolder3$tb_cus": "",
                        "ctl00$ContentPlaceHolder3$ddlprov": "0",
                        "ctl00$ContentPlaceHolder3$tb_area": "",
                        # "ctl00$ContentPlaceHolder3$tb_end": "2018-10-30",
                        "ctl00$ContentPlaceHolder3$tb_end": self.last,
                        "ctl00$ContentPlaceHolder3$ddlgoods": "0",
                        "ctl00$ContentPlaceHolder3$tb_addr": "",
                        "ctl00$ContentPlaceHolder3$txtpagesize1": "15",
                        "ctl00$ContentPlaceHolder3$Text1": j,
                        "ctl00$ContentPlaceHolder3$txtpagesize": "15",
                        "ctl00$ContentPlaceHolder3$pageno": "",
                        "__VIEWSTATEENCRYPTED": "",
                        "__EVENTVALIDATION": self.__EVENTVALIDATIONs,
                        "ctl00$ContentPlaceHolder3$Imagebutton5.x": str(random.randint(2, 26)),
                        "ctl00$ContentPlaceHolder3$Imagebutton5.y": str(random.randint(0, 11)),
                    }

                    post_resp = self.sess_ningbohuadong.post(url='http://lx.eastpharm.com/Flow_Query.aspx',
                                                             headers=self.headers,
                                                             data=post_data)
                    # print('post_resp', post_resp.content.decode('utf-8'))
                    post_resp = etree.HTML(post_resp.content.decode('utf-8'))
                    self.__VIEWSTATEs = post_resp.xpath('//*[@id="__VIEWSTATE"]/@value')[0]
                    self.__EVENTVALIDATIONs = post_resp.xpath('//*[@id="__EVENTVALIDATION"]/@value')[0]
                    data_len = int(len(post_resp.xpath('//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr'))) - 1
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
                            drug_name = post_resp.xpath(
                                '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[4]/text()' % (i + 2))[
                                0].strip()
                            if not drug_name:
                                drug_name = 1
                            # print('drug_name', drug_name)
                        except:
                            drug_name = 1

                        if drug_name != 1:
                            try:
                                # 药品规格
                                drug_specification = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[5]/text()' % (i + 2))[
                                    0].strip()
                                # if '（' in drug_specification:
                                #     drug_specification = drug_specification.split('（')[0]
                                # if '(' in drug_specification:
                                #     drug_specification = drug_specification.split('(')[0]
                            except:
                                drug_specification = ''

                            try:
                                # 生产企业
                                supplier_name = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[6]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                supplier_name = ''

                            try:
                                # 计量单位(瓶,盒等)
                                drug_unit = ''
                            except:
                                drug_unit = ''

                            try:
                                # 销售类型
                                bill_types = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[1]/text()' % (i + 2))[
                                    0].strip()
                                # print('bill_types', bill_types)

                                if '进货' in bill_types:
                                    bill_type = 1
                                elif '进退' in bill_types:
                                    bill_type = 2
                                elif '销退' in bill_types:
                                    bill_type = 4
                                elif '冲差' in bill_types:
                                    bill_type = 5
                                else:
                                    bill_type = 3

                            except:
                                bill_type = 3

                            try:
                                # 出库数量
                                drug_number = round(float(
                                    post_resp.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[15]/text()' % (
                                                i + 2))[
                                        0].strip()))

                            except:
                                drug_number = 0

                            try:
                                # 批号
                                drug_batch = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[11]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                drug_batch = ''

                            try:
                                # 有效期至
                                valid_till = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[12]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                valid_till = '2000-01-01'

                            try:
                                # 地区
                                area = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[19]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                area = ''

                            try:
                                # 客户类型
                                customer_type = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[18]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                customer_type = ''

                            try:
                                # 医院(终端)名称
                                if bill_type == 1 or bill_type == 2:
                                    hospital_name = ''
                                else:
                                    hospital_name = post_resp.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[9]/text()' % (
                                                i + 2))[0].strip()
                            except:
                                hospital_name = ''

                            try:
                                # 医院(终端)地址
                                if bill_type == 1 or bill_type == 2:
                                    hospital_address = ''
                                else:
                                    hospital_address = post_resp.xpath(
                                        '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[17]/text()' % (
                                                i + 2))[
                                        0].strip()
                            except:
                                hospital_address = ''

                            try:
                                # 销售(制单)时间
                                sell_time = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[3]/text()' % (i + 2))[
                                    0].strip()
                            except:
                                sell_time = '2000-01-01'

                            # 创建时间
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            update_time = create_time

                            table_name = 'order_metadata_ningbohuadong'

                            try:
                                # 单价
                                drug_price = post_resp.xpath(
                                    '//*[@id="ctl00_ContentPlaceHolder3_MyGridView1"]/tr[%s]/td[16]/text()' % (i + 2))[
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

                            sql_crm = "insert into order_metadata_ningbohuadong(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_batch, valid_till, area, customer_type, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                            sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version,
                                                          data_type,
                                                          bill_type, drug_name, drug_specification, supplier_name,
                                                          drug_unit,
                                                          abs(drug_number), drug_batch, valid_till, area, customer_type,
                                                          hospital_name,
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

                            self.cursor.execute('select max(id) from order_metadata_ningbohuadong')
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
                        "SELECT count(*) from order_metadata_ningbohuadong WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_ningbohuadong WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '29-ningbohuadong',
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
                print('ningbohuadong-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_ningbohuadong WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_ningbohuadong WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '29-ningbohuadong',
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
