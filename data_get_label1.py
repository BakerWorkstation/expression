#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: gwc
@Description: 对日志打一级标签
@FilePath: /home/sdc/program-56/preHandle.py

comment 数据最多支持三层嵌套   批量 or 实时？  
'''

import time
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
server_dir = config_env["server_dir"]
sys.path.append(os.path.join(server_dir, "env"))

from confluent_kafka import Consumer, KafkaError, Producer, KafkaException, TopicPartition, Message
reload(sys) 
sys.setdefaultencoding('utf8')


from utils.log import record
from utils.pgConnect import pgpool
from utils.redisConnect import redispool

kafka_ip = config_env["kafka_ip"]
kafka_port = config_env["kafka_port"]

logger = record(filename='data_get_label_info.log',
                server_dir=server_dir,
                level=logging.INFO)

elogger = record(filename='data_get_label_error.log',
                server_dir=server_dir,
                level=logging.ERROR)


class Kafkaapi_redis(object):
    def __init__(self,ip_port,groupid,timeout,max_bytes):
        self.kafka_consumer_conf={
        'bootstrap.servers':ip_port,
        'max.poll.interval.ms':3000000,
        'enable.auto.offset.store':False,
        'group.id':groupid,
        'session.timeout.ms':timeout,
        'fetch.message.max.bytes':max_bytes,
        'default.topic.config':{'auto.offset.reset':"smallest"}
    }
        self.kafka_pro_conf={
        'bootstrap.servers': ip_port,
        'message.max.bytes':10485760,
        'retries':3,
        'socket.send.buffer.bytes':10000000,
        'enable.idempotence':True

    }
        self.kafka_pro = Producer(self.kafka_pro_conf)
        self.kafka_cum = Consumer(self.kafka_consumer_conf)
    #回调函数
    def delivery_report(self,err, msg):
        if err is not None:
            #print('Message delivery failed: {}'.format(err))#输出错误信息
            elogger.error('Message delivery failed: {}'.format(err))
            
        else:
            pass
    #生产数据
    def pro_data(self,topic,datas):
        try:
            for data in datas:
                self.kafka_pro.produce(topic,data.encode('utf-8'),callback=self.delivery_report)
            xx=self.kafka_pro.flush(5)
            #print(xx)
            return "ok"
        except Exception as e:
            #print(str(e))
            elogger.error(str(e))
            return "error"

    #自动订阅话题消费 
    def com_data(self,topic,num):
        self.kafka_cum.subscribe([topic])
        datas = self.kafka_cum.consume(num,15)
        return datas
        
    #提交offset
    def com_commit(self,msg):
        #return self.kafka_cum.commit(asynchronous = False)
        return self.kafka_cum.store_offsets(msg)
#连接redis
def redis_con(method,key,value):
    redis_conn = redispool(config_env)
    if method == "get":
        datas = redis_conn.get(key)
    elif method == "set":
        datas = redis_conn.set(key,value)
    elif method == "zadd":
        datas = redis_conn.zadd(key,value)
    elif method == "zaddincr":
        datas = redis_conn.zadd(key,value,incr = True)
    return datas
    
#连接pg数据库 获取数据
def pg_sql(sql,args):
    pg_conn = pgpool(config_env)
    cur = pg_conn.cursor()
    cur.execute(sql,args)
    datas = cur.fetchall()
    pg_conn.close()
    return datas


#连接pg数据库 插入或更新数据
def pg_sql_insert(sql,args):
    pg_conn = pgpool(config_env)
    cur = pg_conn.cursor()
    datas = cur.execute(sql,args)
    pg_conn.commit()
    pg_conn.close()
    return datas
#从数据库中获取一级标签
"""
输入[('sport', 'any', ['asdsad'], 'Yog_demo'), ('label', 'any', ['zxczxczxc'], 'arvada'), ('c.sa.asd', 'contain', ['test'], 'test'), ('direct', 'contain', ['up'], 'uplink'), ('proto', 'contain', ['http'], 'http'), ('sport', 'any', ['aaasd'], ''), ('proto', 'contain', ['pop3'], 'imap')]
输出{'sport': {'type': 'any', 'values': {}, 'label': ''}, 'label': {'type': 'any', 'values': {}, 'label': 'arvada'}, 'c.sa.asd': {'type': 'contain', 'values': {'test': 'test'}, 'label': 'test'}, 'direct': {'type': 'contain', 'values': {'up': 'uplink'}, 'label': 'uplink'}, 'proto': {'type': 'contain', 'values': {'http': 'http'}, 'label': 'http'}}

"""
def get_label1():
    #sql = "select v_key,v_operation,v_value::json,eng_name from h_variable_info where state = '1'"
    sql = "select distinct on(v_key, v_operation, v_value, eng_name) v_key, v_operation,  v_value, eng_name from h_variable_info where state = '1'"
    args = []
    data = pg_sql(sql,args)

    #print(data)
    #处理合并data中数据
    label_datas = {}
    for i in data:
        if label_datas.get(i[0],{}) == {} :
            label_datas[i[0]] = {"type":"","values":{},"label":""}
        if i[1] == "any":
            label_datas[i[0]]["type"] = "any"
            label_datas[i[0]]["label"] = i[3]
        elif label_datas[i[0]]["type"] != "any" and i[1] == "contain":
            label_datas[i[0]]["type"] = "contain"
            #label_datas[i[0]]["values"].update({key:i[3] for key in i[2] })
            label_datas[i[0]]["values"].update({key:i[3] for key in json.loads(i[2]) })
            label_datas[i[0]]["label"] = i[3]
    return label_datas 

#获得规则对应的值
def log_data_key_list(log_data,rule):
    label_list = [log_data]
    change_list = False
    rule_split_list = rule.split(".")
    rule_num=0
    flag_return = False
    while rule_num <len(rule_split_list):
        cnt = 0
        #print(label_list)
        
        while cnt<len(label_list):
            tmp = label_list[cnt]
            if isinstance(tmp,str) or isinstance(tmp,int) or isinstance(tmp,float):
                label_list[cnt] = tmp[rule_split_list[rule_num]] 
                flag_return = True
            elif isinstance(tmp,list):
                label_list[cnt] = [ tmp[num] for num in range(len(tmp))] 
                change_list =True
            elif isinstance(tmp,dict):
                label_list[cnt] = tmp[rule_split_list[rule_num]] 
                if isinstance(tmp[rule_split_list[rule_num]],list):
                    change_list =True
            cnt+=1
        rule_num+=1
        if flag_return:
            break 
        if change_list:
            label_list = [n for m in label_list for n in m ]    
            change_list = False
    #print(label_list)
    return label_list


    
#数据获得一级标签  一次处理一条
def data_get_label(log_data,label_list):  
    #print(label_list)
    for rule,value_to_label in label_list.items():
        try:
            values = log_data_key_list(log_data,rule)
            #print(rule,"         ",values)
            values = list(set(values)-set([""]))
            if value_to_label["type"] == "any" and values != []:
                log_data["labels"].append(value_to_label["label"]) 
                redis_con("zaddincr","origin_test_index",{rule:1})
            elif value_to_label["type"] == "contain":
                for i in values:
                    if i in value_to_label["values"].keys():
                        #print("               33")
                        log_data["labels"].append(value_to_label["values"][i]) 
                        redis_con("zaddincr","origin_test_index",{rule:1})
                        
                        #break
        except Exception as e:
            elogger.error("erro:"+str(e)+"规则匹配失败   规则为："+rule)
            elogger.error("log: "+json.dumps(log_data))
            #print(e)
    return log_data


#对kafka的offset进行提交
def datas_get_label(kfk,msg):
    kfk.com_commit(msg)

    
#获取数据源中数据 kafka json文件 pg库等
def get_datas(kfk):

    while True:
        result = kfk.com_data("exp_origindata",50)
        #print("get")
        #logger("5s时间")
        if result != None and result!= []:
            break
        else:
            logger.info("5s时间没有拉取到数据")
    #kfk.com_commit(result[-1])
    return result
    

#主函数 
def main():
    log_data = json.load(open(os.path.join(server_dir,"log.json")))
    label_list = get_label1()
    kfk = Kafkaapi_redis("%s:%s" % (kafka_ip, kafka_port),"groupid1",6000,1000000)
    #print(label_list)
    while True:
        #log_datas =  [{"b":"tt","c":{"sa":[{"asd":["test1"]}]},"label":[]}]
        
        try:
            log_datas = get_datas(kfk)
            #print(len(log_datas))
            for log_data in log_datas:
                if redis_con("get","label1_update","") == "True":
                    label_list = get_label1()
                    redis_con("set","label1_update","False") 
                if log_data.error():
                    print(log_data.error())
                    continue
                log = json.loads(log_data.value())
                log = data_get_label(log,label_list)
                #print(log_data.offset(),"             ",log_data.partition())
                tmp_time = time.time()
                redis_con("zadd","origin_test",{json.dumps(log):int(tmp_time)})
                
                sql = "insert into h_originlog_info(id,message,rtime) values((select max(id) from h_originlog_info)+1,%s::json,%s)"
                datas = [json.dumps(log),int(tmp_time)]
                result = pg_sql_insert(sql,datas)
                datas_get_label(kfk,log_data)
        except Exception as e:
            elogger.error(str(e))
        #break
    return log_data
   

if __name__ == "__main__":
    #print(redis_con("get","test_g","value"))
    #print(get_label1())
    #print(redis_con("get","label1_update",""))
    #print(redis_con("set","label1_update","True"))
    #print(redis_con("get","label1_update",""))
    #print(redis_con("set","label1_update","False"))
    #a={"b":"c","c":{"sa":[{"asd":"221"}]}}
    ##print(redis_con("get","label1_update",""))
    #kfk = Kafkaapi_redis("10.255.175.92:6667","groupid1",6000,1000000)
    #result = kfk.pro_data("exp_origindata",[json.dumps(a)])
    #print(result)
    ##result = kfk.com_data("exp_origindata",1)
    ##result = kfk.com_data("test_standard",1)
    ##result = kfk.com_data("test_standard",1)
    #result = kfk.com_data("exp_origindata",2)
    #print(json.loads(result[0].value()))
    #print(result[0].offset())
    #print(result[-1].offset())
    #result = kfk.com_data("test_standard1",2)
    ##print(result[0].value())
    #print(result[0].offset())
    #print(result[-1].offset())
    #print(kfk.com_commit(result[-1]))
    ##print(kfk.com_commit())
    #result = kfk.com_data("test_standard1",2)
    #print(result[0].offset())
    #print(result[-1].offset())
    #print(kfk.com_commit(result[-1]))



    # 测试 sql
    #sql = "insert into h_originlog_info values(%s,%s::timestamp,%s::json)"
    #datas = [1,time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(1586273881)),json.dumps(a)]
    ##result = pg_sql_insert(sql,datas)
    #datas = pg_sql("select * from h_originlog_info",[])
    #print(datas[0][2]['b'])


    #print(redis_con("zadd","origin_test",{"sa":32}))
    #print(kfk.com_commit())
    main()
    #test
    #b= {"b":"c","c":{"sa":[{"asd":"test"}]}}
    #a={"b":"c","c":{"sa":[{"asd":"22"}]}}
    #rule = a.get("c",a["c"]).get("sa",[value for value in a.get("c",a["c"]).get("sa")])
    #rule = "c.sa.asd"
    b = {
            "dip": "10.255.49.17",
            "direct": "up",
            "dport": "80",
            "file": [
                {
                    "md5": "",
                    "name": "",
                    "size": "",
                    "suffix": "",
                    "type": "type1"
                },
                {
                    "md5": "",
                    "name": "",
                    "size": "",
                    "suffix": "",
                    "type": "type2"
                },
                {
                    "md5": "",
                    "name": "",
                    "size": "",
                    "suffix": "",
                    "type": "type3"
                }
            ],
            "id": "F07E0BBA82DF5F88A731340397CC0485",
            "labels": [
                "executable_file",
                "Untrusted_address",
                "Not_detected",
                "No_trusted_signature",
                "uplink",
                "Encrypted_or_unrecognized",
                "HTTP",
                "FTP",
                "Descending"
            ],
            "proto": "http",
            "reference": {},
            "sip": "10.255.52.100",
            "sport": "10606",
            "url": ""
        }
    #print(log_data_key_list(b,"file.type"))
    #label_list = [a]
    #has_add_list = 1
    #rule_split_list = rule.split(".")
    #print(rule_split_list)
    #rule_num=0
    #while rule_num <len(rule_split_list):
    #    cnt = 0
    #    while cnt<len(label_list):
    #        tmp = label_list[cnt]
    #        if isinstance(tmp,str) or isinstance(tmp,int) or isinstance(tmp,float):
    #            tmp = tmp[rule_split_list[rule_num]]
    #            label_list[cnt] = tmp 
    #            break
    #        elif isinstance(tmp,list):
    #            tmp = [ tmp[num] for num in range(len(tmp))]
    #            has_add_list +=1
    #        elif isinstance(tmp,dict):
    #            tmp = tmp[rule_split_list[rule_num]]
    #        label_list[cnt] = tmp 
    #        cnt+=1
    #    if has_add_list == 2:
    #        label_list = [n for m in label_list for n in m ]    
    #        has_add_list -=1 
    #        continue
    #    rule_num+=1 
    #    print(rule_num)  
    #print(rule)
    #print(label_list)
    #return label_list
