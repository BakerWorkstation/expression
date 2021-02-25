#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 15:23:23
@LastEditTime: 2020-03-20 20:28:45
@LastEditors: Please set LastEditors
@Description: 构造模拟数据，生成相应的json数据文件
@FilePath: /home/sdc/program-56/utils/ruleUpdate.py
'''


import os
import sys
import uuid
import json
import copy
import random

sys.path.append("/home/demo/dataCollect/")


# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

from utils.redisConnect import redispool

redis_conn = redispool(config_env)


'''
@description: 配置规则更新推送通知
@param {type}
@return:
'''
def sync_update(names=[]):
    try:
        if names:
            for eachname in names:
                cmd = "systemctl restart %s.service" % eachname
                os.system(cmd)
    except:
        pass
    redis_conn.set("webexp", "False")

def label1_sync_update():
    redis_conn.set("label1_update", "True")

def main():
    sync_update()

if __name__ == "__main__":
    main()

