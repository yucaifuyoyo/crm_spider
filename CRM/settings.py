# -*- coding: utf-8 -*-

# Scrapy settings for CRM project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'CRM'

SPIDER_MODULES = ['CRM.spiders']
NEWSPIDER_MODULE = 'CRM.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 320

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'CRM.middlewares.CrmSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'CRM.middlewares.CrmDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'CRM.pipelines.CrmPipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
COMMANDS_MODULE = 'CRM.commands'

# ====================== 公用参数 ==================================
import time
import datetime
import os
import platform
import random

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"}

# 设置延时
# DOWNLOAD_DELAY = random.random() + random.random()
# RANDOMIZE_DOWNLOAD_DELAY = True

# 1表示服务器和本地部署  0表示scrapyd部署
SCRAPYD_TYPE = 1

# ===================== 日 月 流向 ==================================
# 1表示月流向    0表示日流向
MONTHS = 0
if MONTHS == 0:
    YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
    FIST = YESTERDAY
    # FIST = '2018-08-01'
    # FIST = '2018-11-18'
    LAST = YESTERDAY
    # LAST = '2018-10-30'
    # LAST = '2018-11-18'
    # ======================  sql  ==========================
    SQL_CRM_DATA = "insert into stream_pythondata(company_id, delivery_id, delivery_name, table_name, foreign_id, data_version, data_type, bill_type, drug_name, drug_specification, manufacturer_name, drug_hash, drug_unit, drug_number, drug_batch, valid_till, target_name, target_address, target_hash, month, sell_time, stream_hash, create_time, update_time) values('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}')"
    SQL_CRM_RECORD = "insert into stream_python_record(company_id, delivery_id, delivery_name, get_account, spider_name, data_version, get_time, get_date, get_status, data_num, classify_success, remark, create_time, update_time) values('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
    SQL_CRM_VERSION = "insert into data_version(verId, delivery_name, company_id, create_time, update_time, quantity, note) values('{}', '{}', '{}', '{}', '{}', '{}', '{}')"

else:
    YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
    FIST = datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1)
    # FIST = '2018-11-18'
    LAST = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
    # LAST = '2018-11-18'
    # ======================  sql  ==========================
    SQL_CRM_DATA = "insert into stream_pythondata_month(company_id, delivery_id, delivery_name, table_name, foreign_id, data_version, data_type, bill_type, drug_name, drug_specification, manufacturer_name, drug_hash, drug_unit, drug_number, drug_batch, valid_till, target_name, target_address, target_hash, month, sell_time, stream_hash, create_time, update_time) values('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}')"
    SQL_CRM_RECORD = "insert into stream_python_record_month(company_id, delivery_id, delivery_name, get_account, spider_name, data_version, get_time, get_date, get_status, data_num, classify_success, remark, create_time, update_time) values('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
    SQL_CRM_VERSION = "insert into data_version_month(verId, delivery_name, company_id, create_time, update_time, quantity, note) values('{}', '{}', '{}', '{}', '{}', '{}', '{}')"

# ======================= 本地测试 =========================================================================================

# # =========== 本地python Mysql ===========
# HOST = 'localhost'
# PORT = 3306
# USER = 'root'
# PASSWORD = 'root'
# DATABASE = 'pythondata'
#
# # =========== 本地测试python Mysql ===========
# HOST_CRM = 'localhost'
# PORT_CRM = 3306
# USER_CRM = 'root'
# PASSWORD_CRM = 'root'
# DATABASE_CRM = 'crmdata'
#
# # # ========================= crm 数据处理接口 ========================
# CRM_REQUEST_URL = 'http://test.7csc.com/api/streamdata/classify'
# streamType = '1'

# ================ 开发环境测试 =============================================================================================

# # =========== 阿里云python Mysql ===========
# HOST = 'rm-bp13i6zc471eq553puo.mysql.rds.aliyuncs.com'
# PORT = 3306
# USER = 'crmdev'
# PASSWORD = '309fJOJHJdkdu'
# DATABASE = 'pythondata'
# DATABASE = 'pythondata_month'
#
# # =========== 阿里云线上python Mysql ===========
# HOST_CRM = 'rm-bp13i6zc471eq553puo.mysql.rds.aliyuncs.com'
# PORT_CRM = 3306
# USER_CRM = 'crmdev'
# PASSWORD_CRM = '309fJOJHJdkdu'
# DATABASE_CRM = 'crmdata'

# ============== 保存log信息 ===================================
# if 'indow' in platform.system():
#     symbol = r'\\'
# else:
#     symbol = r'/'
# path = os.path.dirname(os.path.dirname(__file__))
# LOG_FILE = "{}{}{}_crm.log".format(path, symbol, YESTERDAY)
# LOG_LEVEL = "INFO"
# LOG_STDOUT = True
# LOG_ENCODING = 'utf-8'

# =================== crm 数据处理接口 =======================
# CRM_REQUEST_URL = 'http://crmapi.7csc.com/api/streamdata/classify'
# streamType = '1'


# ================ 测试环境测试 ============================================================================================

# =========== 阿里云python Mysql ===========
HOST = 'rm-bp13i6zc471eq553puo.mysql.rds.aliyuncs.com'
PORT = 3306
USER = 'crmdev'
PASSWORD = '309fJOJHJdkdu'
DATABASE = 'pythondata'
# DATABASE = 'pythondata_month'

# =========== 阿里云线上python Mysql ===========
HOST_CRM = 'rm-bp13i6zc471eq553puo.mysql.rds.aliyuncs.com'
PORT_CRM = 3306
USER_CRM = 'crmdev'
PASSWORD_CRM = '309fJOJHJdkdu'
# DATABASE_CRM = 'prodcrmdata'
DATABASE_CRM = 'crmtestdata'

# ============== 保存log信息 ===================================
# if 'indow' in platform.system():
#     symbol = r'\\'
# else:
#     symbol = r'/'
# path = os.path.dirname(os.path.dirname(__file__))
# LOG_FILE = "{}{}{}_crm.log".format(path, symbol, YESTERDAY)
# LOG_LEVEL = "INFO"
# LOG_STDOUT = True
# LOG_ENCODING = 'utf-8'

# ========================= crm 数据处理接口 ========================
# CRM_REQUEST_URL = 'http://test.7csc.com/api/streamdata/classify'
CRM_REQUEST_URL = 'https://www.baidu.com/'
streamType = '1'
