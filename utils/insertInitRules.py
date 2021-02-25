#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 15:23:23
@LastEditTime: 2020-03-20 20:28:45
@LastEditors: Please set LastEditors
@Description: 往PG中h_rule_info表预先插入初始化一级标签
@FilePath: /home/sdc/program-56/utils/insertInitRules.py
'''


import os
import sys
import uuid
import time
import json
import copy
import random

sys.path.append("/home/demo/dataCollect")

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()
server_dir = config_env["server_dir"]

from utils.pgConnect import pgpool
#from utils.redisConnect import redispool

pg_conn = pgpool(config_env)
#redis_conn = redispool(config_env)


sys.path.append(os.path.join(server_dir, "../accept_new_rules"))
from rule_wait import level_1_rule_import


'''
@description: 装载json文件
@param {type}
@return:
'''
def loadJsonFile(filename):
    with open(os.path.join(server_dir, "models/%s" % filename), 'r') as f:
        data_back = json.load(f)
    return data_back


'''
@description: 初始化一级标签库，加载标签json文件，录入PG中
@param {type}
@return:
'''
def initLabel():
    cur = pg_conn.cursor()
    filename = 'labels2.json'
    data = loadJsonFile(filename)
    timestamp = int(time.time())
    flag = True
    sql = "insert into h_rule_info(eng_name, chn_name, state, express, type, source, rtime) values (%s, %s, %s, %s, %s, %s, %s)"
    for eachitem in data["data"]:
        #print(eachitem)
        #en_name = eachitem["en_name"]
        #exp = eachitem["exp"]
        cn_name = eachitem["cn_name"]
        response = level_1_rule_import(cn_name)
        if not response["success"]:
            print(response)
            flag = False
            break
        en_name = response["label_english"]
        exp = "%s_rule(data)" % en_name
        state = "1"
        type = "1"
        source = "1"
        args = (en_name, cn_name, state, exp, type, source, timestamp,)
        cur.execute(sql, args)
    if flag:
        print(u"标签录入成功")
        pg_conn.commit()


'''
@description:  主函数
@param {type}
@return:
'''
def main():
    initLabel()


'''
@description: 入口函数
@param {type}
@return:
'''
if __name__ == "__main__":
    main()

