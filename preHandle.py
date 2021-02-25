#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-21 11:03:13
@LastEditTime: 2020-03-21 11:35:29
@LastEditors: Please set LastEditors
@Description: 对原始日志进行预处理
@FilePath: /home/sdc/program-56/preHandle.py
'''


import re
import os
import sys
import json
import time
import copy
import logging

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

from utils.log import record
from utils.pgConnect import pgpool
from utils.redisConnect import redispool

server_dir = config_env["server_dir"]
sys.path.append(os.path.join(server_dir, "env"))

wait = config_env["wait"]

logger = record(filename='preHandle_server.log',
                server_dir=server_dir,
                level=logging.INFO)

elogger = record(filename='preHandle_error.log',
                server_dir=server_dir,
                level=logging.ERROR)

#sys.path.append(os.path.join(server_dir, "../rule_conf/"))
#from expression_rule import *


'''
@description: 递归函数 统计当前标签所有子集标签
@param {type}
@return:
'''
def recure_label(mapLabel, TrueList, wholeList, one_level):
    for eachlabel in wholeList:
        try:
            childlabel = mapLabel[eachlabel]
        except:
            #  找不到映射关系，说明是第一级标签，没有子级
            one_level.add(eachlabel)
            continue
        if [eachlabel] == mapLabel[eachlabel]:
            #  找到映射关系还是自身，说明是第一级标签，没有子级
            one_level.add(eachlabel)
            continue
        for eachitem in childlabel:
            try:
                TrueList.remove(eachitem)
            except:
                continue
        recure_label(mapLabel, TrueList, childlabel, one_level)
    return TrueList, one_level


'''
@description:  从数据库中加载所有规则提取标签集合
@param {type} 
@return: 
'''
def loadrule():
    labels = set()
    #for key, value in globals().items():
    #    if '_rule' in str(value):
    #        for eachlabel in re.findall('\w+_rule', value):
    #            labels.add(eachlabel)
    #origin_rule = set(map(lambda x: x.split("_rule")[0], labels))
    #return origin_rule
    pg_conn = pgpool(config_env)
    redis_conn = redispool(config_env)
    cur = pg_conn.cursor()
    #  查询表达式库中所有启用的表达式，state='1' 为启用，  type='1' 为一级标签   type>'1' 为表达式
    #getLabel = "select eng_name, express from h_rule_info where state='1' and type>'1'"
    getLabel = "select eng_name, express from h_rule_info where type>'1'"
    cur.execute(getLabel)
    data = cur.fetchall()
    if not data:
        logger.warning(u'表达式规则全部停用')
        redis_conn.set("webexp", "True")
        pg_conn.close()
        redis_conn.close()
        return labels

    labelDict = {}
    TrueList = set()
    rule_onelevel = set()
    for eachrule in data:
        label = eachrule[0]  # 表达式名称，二级标签名
        exp = eachrule[1]    # 表达式具体规则详情
        #map_label = map(lambda x: x[: x.rfind('_')], re.findall(r'([!\w]+_bin|[!\w]+_rule)\(data\)', exp)) # 找出表达式包含的标签列表
        map_label = map(lambda x:x[0], list(filter(lambda x: 'not ' not in x[0] , re.findall(r'(not \w+|\w+)(_bin|_rule)\(data\)', exp)))) # 找出表达式包含的标签列表
        labelDict[label] = map_label  # 建立映射关系
        TrueList.add(label)           # 结果为True的集合增加该标签成员
        TrueList = TrueList | set(map_label)  # 构造全量数据，包含所有子父关系的标签
    wholeList = copy.deepcopy(TrueList)
    endLabel, labels = recure_label(labelDict, TrueList, wholeList, rule_onelevel) # 递归查询所有子父关系的标签, 统计相关数据(不保留
    redis_conn.set("webexp", "True")
    pg_conn.close()
    redis_conn.close()
    return labels


'''
@description:  从redis(key: origin_data)中读出数据，按配置的表达式提取的标签集合，
            对日志进行初步过滤，日志中的标签不在标签集合，该日志跳过不做处理。
@param {type} 
@return: 
'''
def handle():
    global confLabels
    redis_conn = redispool(config_env)
    pg_conn = pgpool(config_env)
    cur = pg_conn.cursor()
    # 获取原始日志队列指针 —— 数据评分
    score = redis_conn.get("start_score")
    if not score:
        score = 0
    else:
        score = int(score)
    # 预留一个key，规则配置发生改变时，重新加载加载
    webrule = redis_conn.get("webexp")
    if not webrule == "True":
        # 从pg中获取标签规则, 规则的启用状态
        logger.warning(u'警告 -> 表达式更新，重新加载...')
        confLabels = loadrule()
    if not confLabels:
        logger.warning(u'没有匹配到一级标签(未创建表达式)，缓存池休眠')
        pg_conn.close()
        return False
 
    timestamp = int(time.time())
    #data = redis_conn.zrangebyscore("origin_data", score, timestamp, withscores=True)      # 按评分由低到高
    #data = redis_conn.zrevrangebyscore("origin_data", timestamp, score, withscores=True)  # 按评分由高到低
    data = []
    # -----20200326----- 修改为从PG库中查询原始日志，进行缓存池预处理
    sql = "select message from h_originlog_info where rtime>=%s and rtime<%s"
    args = (score, timestamp, )
    cur.execute(sql, args)
    data = cur.fetchall()
    score = timestamp
    redis_conn.set("start_score", score)
    for eachdata in data:
        logger.info(u'预处理日志 -> %s' % eachdata[0])
        labels = set(json.loads(eachdata[0])["labels"])
        logger.info(u'日志包含标签集合 -> %s' % str(labels))
        logger.info(u'配置文件标签集合 -> %s' % str(confLabels))
        mix = labels & confLabels
        logger.info(u'标签交集集合 -> %s' % str(mix))
        if mix:
            redis_conn.rpush("cachequeue", eachdata[0])
            logger.warning(u'匹配结果 -> True')
        else:
            logger.warning(u'匹配结果 -> False')
        logger.info('-' * 80)

    pg_conn.close()
    return True

def main():
    global confLabels
    confLabels = loadrule()
    while 1:
        handle()
        time.sleep(wait)

if __name__ == "__main__":
    confLabels = set()
    main()
