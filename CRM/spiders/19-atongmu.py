# -*- coding: utf-8 -*-
import datetime
import hashlib
import time
import pymysql
import requests
import scrapy
from lxml import etree
import json
import platform
from CRM.settings import HOST, PORT, PASSWORD, USER, DATABASE, HOST_CRM, PORT_CRM, PASSWORD_CRM, USER_CRM, DATABASE_CRM, \
    SQL_CRM_DATA, SQL_CRM_RECORD, SQL_CRM_VERSION, HEADERS, YESTERDAY, FIST, LAST, CRM_REQUEST_URL, streamType, MONTHS, \
    SCRAPYD_TYPE


class AtongmuSpider(scrapy.Spider):
    name = 'atongmu'
    # allowed_domains = ['.com']
    start_urls = ['http://202.107.195.85:8087/productflow/login.htm']
    headers = HEADERS
    yesterday = YESTERDAY
    fist = FIST
    last = LAST
    headers_post = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Content-Type": "application/json; charset=utf-8",
    }
    sess_atongmu = requests.session()
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
        # delivery_id = 'F617B115D6F3447983E94BB781231248'
        delivery_id = 'DDA100100Y'
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
            html = self.sess_atongmu.get(url=self.start_urls[0], headers=self.headers, verify=False)

            data = {
                "username": get_account,
                "password": get_pwd,
            }
            post_html = self.sess_atongmu.post(url='http://202.107.195.85:8087/productflow/checkUserAndPwd.htm',
                                               data=data,
                                               headers=self.headers,
                                               verify=False)
            # print('*' * 1000)
            # print('post_html', post_html.content.decode('utf-8'))
            # print('*' * 1000)
            data_html = self.sess_atongmu.get(url='http://202.107.195.85:8087/productflow/product/queryPH.htm',
                                              headers=self.headers, verify=False)
            # print('*' * 1000)
            # print('data_html', data_html.content.decode('utf-8'))
            # print('*' * 1000)
            # data_htmls_bian = self.sess_atongmu.get(
            #     url='http://202.107.195.85:8087/productflow/product/queryPH.htm?spbm=&startdate=2018-01-01&enddate=2018-10-25&ksbm=&stockchangemode=0&batchnumber=&tym=&dzbm=',
            #     headers=self.headers, verify=False)
            data_htmls_bian = self.sess_atongmu.get(
                url='http://202.107.195.85:8087/productflow/product/queryPH.htm?spbm=&startdate={}&enddate={}&ksbm=&stockchangemode=0&batchnumber=&tym=&dzbm='.format(
                    self.fist, self.last), headers=self.headers, verify=False)
            # print('http://202.107.195.85:8087/productflow/product/queryPH.htm?spbm=&startdate={}&enddate={}&ksbm=&stockchangemode=0&batchnumber=&tym=&dzbm='.format(self.fist, self.last))
            # print('*' * 1000)
            # print('data_htmls_bian', data_htmls_bian.content.decode('utf-8'))
            # print('*' * 1000)
            try:
                data_resps = etree.HTML(data_htmls_bian.content.decode('utf-8'))
                data_len = int(len(data_resps.xpath('//*[@id="showcontent"]/table/tr'))) - 1
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
                        drug_name = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[4]/text()' % (i + 2))[
                            0].strip()
                        if not drug_name:
                            drug_name = 1
                    except:
                        drug_name = 1

                    if drug_name != 1:
                        try:
                            # 药品规格
                            drug_specification = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[6]/text()' % (i + 2))[
                                    0].strip()
                        except:
                            drug_specification = ''

                        try:
                            # 生产企业
                            supplier_name = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[8]/text()' % (i + 2))[
                                    0].strip()
                        except:
                            supplier_name = ''

                        try:
                            # 计量单位(瓶,盒等)
                            drug_unit = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[7]/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_unit = ''

                        try:
                            # 销售类型
                            bill_types = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[2]/text()' % (i + 2))[
                                0].strip()
                            # print('bill_types', bill_types)

                            if bill_types == '进货(购进)':
                                bill_type = 1
                            elif '冲账' in bill_types:
                                bill_type = 5
                            elif bill_types == '进货(退出)':
                                bill_type = 2
                            elif bill_types == '销售(退回)':
                                bill_type = 4
                            elif bill_types == '报损':
                                bill_type = 3
                            else:
                                bill_type = 3

                        except:
                            bill_type = 3

                        try:
                            # 出库数量
                            drug_number = int(
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[12]/text()' % (i + 2))[
                                    0].strip())

                        except:
                            drug_number = 0

                        try:
                            # 药品产地
                            drug_area = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[9]/text()' % (i + 2))[
                                0].strip()
                        except:
                            drug_area = ''

                        try:
                            # 批号
                            drug_batch = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[11]/text()' % (i + 2))[
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
                            if bill_type == 1 or bill_type == 2:
                                hospital_name = ''
                            else:
                                hospital_name = \
                                    data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[3]/text()' % (i + 2))[
                                        0].strip()
                        except:
                            hospital_name = ''

                        try:
                            # 医院(终端)地址
                            if bill_type == 1 or bill_type == 2:
                                hospital_address = ''
                            else:
                                hospital_address = \
                                    data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[15]/text()' % (i + 2))[
                                        0].strip()
                        except:
                            hospital_address = ''

                        try:
                            # 销售(制单)时间
                            sell_time = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[1]/text()' % (i + 2))[
                                0].strip()
                        except:
                            sell_time = '2000-01-01'

                        # 创建时间
                        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        update_time = create_time

                        table_name = 'order_metadata_atongmu'

                        try:
                            # 单价
                            drug_price = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[14]/text()' % (i + 2))[
                                    0].strip()
                        except:
                            drug_price = ''
                        try:
                            # 供应商名称
                            channel_name = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[16]/text()' % (i + 2))[
                                    0].strip()
                        except:
                            channel_name = ''

                        try:
                            # 商品名称
                            goods_name = data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[5]/text()' % (i + 2))[
                                0].strip()
                        except:
                            goods_name = ''

                        try:
                            # 订单号
                            order_number = \
                                data_resps.xpath('//*[@id="showcontent"]/table/tr[%s]/td[10]/text()' % (i + 2))[
                                    0].strip()
                        except:
                            order_number = ''

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

                        sql_crm = "insert into order_metadata_atongmu(company_id, delivery_id, delivery_name, data_version, data_type, bill_type, drug_name, drug_specification, supplier_name, drug_unit, drug_number, drug_area, drug_batch, valid_till, hospital_name, hospital_address, sell_time, create_time, update_time, drug_price, channel_name, goods_name, order_number, drug_hash, hospital_hash, stream_hash, month) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {})"
                        sql_data_crm = sql_crm.format(company_id, delivery_id, delivery_name, data_version, data_type,
                                                      bill_type, drug_name, drug_specification, supplier_name,
                                                      drug_unit,
                                                      abs(drug_number), drug_area, drug_batch, valid_till,
                                                      hospital_name,
                                                      hospital_address, sell_time, create_time, update_time,
                                                      drug_price,
                                                      channel_name, goods_name, order_number, drug_hash,
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

                        self.cursor.execute('select max(id) from order_metadata_atongmu')
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
                        "SELECT count(*) from order_metadata_atongmu WHERE sell_time='{}' and delivery_name='{}'".format(
                            self.yesterday, enterprise_name))
                else:
                    month = int(str(self.fist).replace('-', '')[0: 6])
                    self.cursor.execute(
                        "SELECT count(*) from order_metadata_atongmu WHERE month='{}' and delivery_name='{}'".format(
                            month, enterprise_name))
                data_num = self.cursor.fetchone()[0]
                remark = ''
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                update_time = create_time
                sql_crm_record = SQL_CRM_RECORD
                sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name,
                                                            get_account, '19-atongmu',
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
                print('atongmu-登入失败:%s' % e)
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
                            "SELECT count(*) from order_metadata_atongmu WHERE sell_time='{}' and delivery_name='{}'".format(
                                self.yesterday, enterprise_name))
                    else:
                        month = int(str(self.fist).replace('-', '')[0: 6])
                        self.cursor.execute(
                            "SELECT count(*) from order_metadata_atongmu WHERE month='{}' and delivery_name='{}'".format(
                                month, enterprise_name))
                    data_num = self.cursor.fetchone()[0]
                    remark = '账号或密码错了'
                    update_time = create_time
                    sql_crm_record = SQL_CRM_RECORD
                    sql_data_crm_record = sql_crm_record.format(company_id, delivery_id, enterprise_name, get_account,
                                                                '19-atongmu',
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
