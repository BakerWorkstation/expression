#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 14:16:59
@LastEditTime: 2020-03-21 11:12:37
@LastEditors: Please set LastEditors
@Description: 读取json数据源，录入redis库中
@FilePath: /home/sdc/program-56/createData.py
'''


import os
import sys
import json
import time
import uuid
import random
import logging

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

from utils.createJson import *  # icmp, ddos, ports
from utils.log import record
from utils.pgConnect import pgpool
from utils.redisConnect import redispool
from multiprocessing import Pool

server_dir = config_env["server_dir"]
sys.path.append(os.path.join(server_dir, "env"))


'''
@description:  装载json文件
@param {type}  
@return: 
'''
def loadJsonFile(filename):
    with open(os.path.join(server_dir, "models/%s" % filename), 'r') as f:
        data_back = json.load(f)
    return data_back


'''
@description:  把JSON文件中的数据录入到redis(key: origin_data)中，把时间戳当评分
@param {type}  
@return: 
'''
def in2Redis(data):
    logger = record(filename='createData_server.log',
                   server_dir=server_dir,
                   level=logging.INFO)

    elogger = record(filename='createData_error.log',
                   server_dir=server_dir,
                   level=logging.ERROR)
    pg_conn = pgpool(config_env)
    sql = "insert into h_originlog_info(logid, message, rtime) values(%s, %s, %s)"
    cur = pg_conn.cursor()
    redis_conn = redispool(config_env)
    for eachdata in data["data"]:
        try:
            eachdata["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
            redis_conn.zadd("origin_data", {json.dumps(eachdata): int(time.time())})
            args = (eachdata["id"], json.dumps(eachdata), int(time.time()), )
            cur.execute(sql, args)
            pg_conn.commit()
        except Exception as e:
            elogger.error(str(e))
            sys.exit(0)
        logger.info(u'构造日志 -> %s' % eachdata)
        logger.info('-' * 50)
        #time.sleep(random.randint(90, 120))
        time.sleep(1)

    pg_conn.close()
    #data = redis_conn.zrange("mylist", 0, -1, "withscores")
    #for eachdata in data:
    #    print(eachdata)


'''
@description:  主函数
@param {type}  
@return: 
'''
def main():

    # 处理icmp网段扫描数据
    icmp()
    filename = "scan_ips.json"
    ipsdata = loadJsonFile(filename)
    #in2Redis(ipsdata)

    # 处理tcp端口扫描数据
    ports()
    filename = "scan_ports.json"
    portdata = loadJsonFile(filename)
    #in2Redis(portdata)
    
    # 处理ddos数据
    ddos()
    filename = "ddos.json"
    ddosdata = loadJsonFile(filename)
    #in2Redis(ddosdata)

    # 处理钓鱼邮件数据
    filename = "fish_email.json"
    fishdata = loadJsonFile(filename)

    # 处理垃圾邮件数据
    filename = "rubbish_email.json"
    rubbishdata = loadJsonFile(filename)

    # 处理远程powershell数据 
    filename = "remote_powershell.json"
    shelldata = loadJsonFile(filename)

    #mission = [ipsdata, portdata, ddosdata]
    mission = [fishdata, rubbishdata, shelldata]
    p = Pool(len(mission))
    for params in mission:
        p.apply_async(in2Redis, args=(params, ))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    


'''
@description:  入口函数
@param {type}  
@return: 
'''
if __name__ == "__main__":
    while 1:
        main()
        time.sleep(10)
