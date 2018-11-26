#! /bin/bash

# crontab -e: 每隔6小时执行一次    0 */6 * * * sh /PythonSpiderScrapy/scrapy_venv/NewSpider/crm_crontab.sh

# 指定scrapy目录
export PATH=$PATH:/usr/local/bin

# 指定程序所在目录（绝对路径)
cd /root/crm/08-CRM


today=$(date +"%Y-%m-%d")
nohup scrapy crawlall >> ./gov_crm_$today.log 2>&1 &


