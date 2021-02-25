#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-21 11:03:13
@LastEditTime: 2020-03-21 11:35:29
@LastEditors: Please set LastEditors
@Description: 将符合过滤条件的日志 执行表达式，获取二级标签
@FilePath: /home/sdc/program-56/execExpression.py
'''


import re
import os
import sys
import copy
import json
import time
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

eogger = record(filename='execExp_server.log',
                server_dir=server_dir,
                level=logging.INFO)
elogger = record(filename='execExp_error.log',
                server_dir=server_dir,
                level=logging.ERROR)

#  加载表达式可执行文件目录
binpath = os.path.join(server_dir, "../bin_rule")
sys.path.append(binpath)


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
@description: 从PG库中提取所有表达式，统计True对应字段
@param {type} 
@return: 
'''
def execRule():
    import importlib
    redis_conn = redispool(config_env)
    config_env["redis_db"] = 6
    redis_conn_scene = redispool(config_env)
    #pg_conn = pgpool(config_env)
    addLabel = "insert into h_loglabel_info(logid, label_without_child, label_with_child, log_onelevel, rule_onelevel, show_onelevel, rtime) values (%s, %s, %s, %s, %s, %s, %s)"

    labelDict = {}       #  标签库中标签名和表达式中包含所有标签的映射关系  例:  download : ["http", "up"]
    TrueList = set()     #  定义执行表达式，返回结果为True的表达式名称(二级标签)的集合
    rule_onelevel = set()    #  执行成功的表达式包含所有一级标签的集合
    pg_conn = pgpool(config_env)
    cur = pg_conn.cursor()
    #  查询表达式库中所有启用的表达式，state='1' 为启用，  type='1' 为一级标签   type>'1' 为表达式
    #getLabel = "select eng_name, express from h_rule_info where state='1' and type>'1' "
    getLabel = "select eng_name, express, state from h_rule_info where type>'1' "
    cur.execute(getLabel)
    data = cur.fetchall()
    if not data:
        logger.warning(u'表达式规则全部停用，不处理数据...')
        time.sleep(5)
        return False

    # 从缓存池队列中消费处理
    origin_data = redis_conn.blpop("cachequeue", timeout=0)[-1]
    logdata = json.loads(origin_data)
    logger.info(u"处理日志 -> %s" % logdata)
    logger.info(u"日志编号 -> %s" % logdata["id"])
    timestamp = int(time.time())

    for eachrule in data:
        label = eachrule[0]  # 表达式名称，二级标签名
        exp = eachrule[1]    # 表达式具体规则详情
        state = eachrule[2]    # 表达式启用状态  "1": 启用
        #if label in TrueList:
        #    continue
        #map_label = map(lambda x: x[: x.rfind('_')], re.findall(r'(\w+_bin|\w+_rule)\(data\)', exp)) # 找出表达式包含的标签列表
        map_label = map(lambda x:x[0], list(filter(lambda x: 'not ' not in x[0] , re.findall(r'(not \w+|\w+)(_bin|_rule)\(data\)', exp)))) # 找出表达式包含的标签列表
        # print(map_label)
        labelDict[label] = map_label  # 建立映射关系
        if not state == "1":
            # 表达式停用，不执行
            continue
        libname = label + '_bin'  # 表达式文件名  (http_bin.py)
        try:
            model = importlib.import_module(libname)
            libfunction = getattr(model, libname)
            response = libfunction(logdata)
            #logger.info("表达式返回: %s" % str(response))
            #chinesename = response["label"]
            #event = response["event_details"]
            flag = response#["whether_to_hit"]
            if flag:
                TrueList.add(label)           # 结果为True的集合增加该标签成员
                TrueList = TrueList | set(map_label)  # 构造全量数据，包含所有子父关系的标签
        except Exception as e:
            elogger.error(u"Error: 调用表达式出错, message: %s "% str(e))
            # 统计失败时间点，将失败的表达式名称 放入redis有序集合
            score = redis_conn.zscore("expError", label)
            if not score:
                redis_conn.zadd("expError", {label: int(time.time())})
            else:
                if int(time.time()) > int(score) + 60:
                    # 表达式出错已超时，同步pg 更新表达式状态为有误
                    # pg_conn

                    redis_conn.zrem("expError", label)
                
            continue
         
    if TrueList:
        wholeList = copy.deepcopy(TrueList)   
        endLabel, rule_onelevel = recure_label(labelDict, TrueList, wholeList, rule_onelevel) # 递归查询所有子父关系的标签, 统计相关数据(不保留子父关系、保留子父关系、所有包含一级)标签
        logger.info(u'不保留父子关系标签: %s' % endLabel)
        logger.info(u'规则包含一级标签: %s' % rule_onelevel)
        logger.info(u'保留父子关系标签: %s'% wholeList)

        # 二级标签匹配成功，将日志id和二级标签对应关系存入redis 集合中
        #redis_conn.zadd(labelname, {logdata["id"]: int(time.time())})
        log_onelevel = set(logdata["labels"])   # 原始日志自带的一级标签集合
        logger.info(u'原始日志自带一级标签: %s' % log_onelevel)
        diff_one = log_onelevel - rule_onelevel   #  做差集, 过滤标签包含的一级，保留没有被二级标签引用的一级标签
        logger.info(u'过滤后的一级标签: %s' % diff_one)
        args = (logdata["id"], json.dumps(list(endLabel)), json.dumps(list(wholeList)), json.dumps(list(log_onelevel)), json.dumps(list(rule_onelevel)), json.dumps(list(diff_one)), timestamp,)
	cur.execute(addLabel, args)
        pg_conn.commit()
        logger.info('-' * 50)
        #redis_conn_scene.hgetall("rule_scene")
        return True
    return False


def main():
    while 1:
        execRule()

if __name__ == "__main__":
    main()
