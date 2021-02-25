#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2019-12-18 11:03:51
@LastEditTime: 2020-04-28 11:24:52
@LastEditors: Please set LastEditors
@Description: IEP relate data to ACD
@FilePath: /opt/kafkaConsu/consume1.py
'''


import os
import sys
# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()
server_dir = config_env["server_dir"]
sys.path.append(os.path.join(server_dir, "env"))
import time
import json
import uuid
import redis
import base64
import logging
import datetime
import psycopg2
import requests
import threading
import psycopg2.extras
import confluent_kafka
from utils.pgConnect import pgpool
from utils.redisConnect import redispool
from multiprocessing import Process
from confluent_kafka import Consumer, KafkaError, TopicPartition
#import psycopg2.pool
#from DBUtils.PooledDB import PooledDB

from utils.log import record


logger = record(filename='consume_server.log',
               server_dir= os.path.join(server_dir, 'logs/consume_service'),
               level=logging.INFO)

elogger = record(filename='consume_error.log',
               server_dir= os.path.join(server_dir, 'logs/consume_service'),
               level=logging.ERROR)


main_thread_lock = threading.Lock()

'''
@description:    kafka生产者回调函数
@param {type}    err(object),  message(string)
@return:
'''
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


'''
@description:    kafka消费者处理数据函数
@param {type}    topic(string),  partition(int),  functions(object),  redis_conn(object),  pg_conn(object),  config(dict),  logger(object)
@return:
'''
def consumeData(topic, partition, args):
    functions = args[0]
    redis_conn = args[1]
    pg_conn = args[2]

    offsetkey = "%s_%s" % (topic, partition)
    redis_offset = redis_conn.get(offsetkey)
    broker_list = "%s:%s" % (config_env["kafka_ip"], config_env["kafka_port"])
    producer = confluent_kafka.Producer({"bootstrap.servers": broker_list})
    tp_c = TopicPartition(topic, partition, 0)
    consume = Consumer({
                        'bootstrap.servers': broker_list,
                        'group.id': config_env["kafka_group_id"],
                        'enable.auto.commit': False,
                        'max.poll.interval.ms': config_env["kafka_max_poll"],
                        'default.topic.config': {'auto.offset.reset': config_env["kafka_reset"]}
    })
    # 获取数据对应最小offset 与 redis记录中的offset比较
    kafka_offset = consume.get_watermark_offsets(tp_c)[0]
    if not redis_offset:
        offset = kafka_offset
    else:
        if int(redis_offset) > kafka_offset:
            offset = int(redis_offset)
        else:
            offset = kafka_offset
    # 重新绑定offset 消费
    tp_c = TopicPartition(topic, partition, offset)
    consume.assign([tp_c])
    data = consume.consume(config_env["length"], 3)
    write_offset = offset
    if data:
        logger.info("topic: %s  partition: %s\t  data_length : %s" % (topic, partition, len(data)))
        for eachmsg in data:
            if eachmsg.error():
                elogger.error('error: %s' % eachmsg.error())
                write_offset += 1
                continue

            # 处理日志数据函数  flag: 'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功)
            args = (redis_conn, pg_conn, )
            flag, message = functions[topic](eachmsg.value(), args)
            write_offset += 1
            # 数据解析失败，需要往新话题写入数据
            if flag == 'parse':
                elogger.error(message)
                producer.produce('parse_error', eachmsg.value())
                producer.flush(timeout=1) #  0: 成功  1: 失败
            # 数据处理失败，需要往原话题写回数据
            elif flag == 'error':
                elogger.error(message)
                producer.produce(topic, eachmsg.value())
                producer.flush(timeout=1) #  0: 成功  1: 失败
            # 数据处理成功
            elif flag == 'success':
                logger.info('-' * 50)
                logger.info(message)
                producer.produce(topic + '_exp', message)
                flush_flag = producer.flush()
                logger.info("%s_exp flush: %s" % (topic, flush_flag))
            else:
                pass
            # with main_thread_lock:
            #     pass
    else:
        logger.info(u"topic: %s  partition: %s\t无数据" % (topic, partition))
    # 处理结束后， redis中更新offset
    tp_c = TopicPartition(topic, partition, write_offset)
    # 获取当前分区偏移量
    kafka_offset = consume.position([tp_c])[0].offset
    # 当前分区有消费的数据, 存在偏移量
    if kafka_offset >= 0:
        # 当redis维护的offset发成超限时，重置offset
        if write_offset > kafka_offset:
            write_offset = kafka_offset
    redis_conn.set(offsetkey, write_offset)
    # 手动提交offset
    consume.commit(offsets=[tp_c])


'''
@description:    kafka阻塞式消费数据
@param {type}    topic(string),  partition(int),  functions(object),  redis_conn(object),  pg_conn(object),  config(dict)
@return:
'''
def reset_offset(topic, partition, args):
    while True:
        try:
            consumeData(topic, partition, args)
            time.sleep(1)
        except Exception as e:
            elogger.error("Error: consumeData function -> message: %s" % str(e))
            continue


'''
@description:    进程对应一个话题，进程开启多线程对应话题分区数量，同时消费数据
@param {type}    topic(string),  partition(int),  config(dict)
@return:
'''
def threatsConsume(topic, functions): 
    logger.info('Run child process (%s)...' % (os.getpid()))
    # 子进程启动多线程方式消费当前分配的话题数据，线程数和分区数要匹配
    # 开启pg连接池
    pg_conn = pgpool(config_env)
    # 开启redis连接池
    redis_conn = redispool(config_env)
    threads = []
    try:
        for partition in range(config_env["kafka_partitions"]):
            args = (functions, redis_conn, pg_conn, )
            child_thread = threading.Thread(
                                            target=reset_offset,
                                            args=(topic, partition, args),
                                            name='LoopThread'
            )
            threads.append(child_thread)
        for eachthread in threads:
            eachthread.start()
        for eachthread in threads:
            eachthread.join()
        logger.info("exit program with 0")
    except Exception as e:
        elogger.error("Error: threatsConsume function threads fail-> message: %s" % str(e))
        sys.exit(0)


'''
@description:    消费采集产生的标准化数据，过表达式引擎，生成对应标签
@param {type}    message(string),  *args
@return:     success/parse/error,  message(string)
'''
def convert_log(message, args):
    redis_conn = args[0]
    #pg_conn = args[1]
    try:
        message = json.loads(message.encode("utf-8"))
        device = message["detect"]["detect_pro"]
        logid = message["id"]
    except Exception as e:
        return 'parse', str(e)

    #  如IEP过滤， 提取五元组信息，输入写入redis 有序集合
    if device == "PTD":
        # 源IP
        try:
            sip = message["src"]["ip_info"]["ip"]
        except:
            sip = None
        # 源端口
        try:
            sport = message["src"]["port"]
        except:
            sport = None
        # 目的IP
        try:
            dip = message["dst"]["ip_info"]["ip"]
        except:
            dip = None 
        # 目的端口
        try:
            dport = message["dst"]["port"]
        except:
            dport = None 
        # 协议
        try:
            protocol = message["carrier"]["protocol"]
        except:
            protocol = None 
        # 日志发现时间做为redis有序集合评分
        try:
            detect_time =  message["detect"]["detect_time"]
            timestamp = int(time.mktime(time.strptime(detect_time, "%Y-%m-%d %H:%M:%S")))
        except:
            timestamp = int(detect_time)
        # 构造简易数据模板，写入redis有序集合
        model = {
                 "id": logid,
                 "sip": sip,
                 "sport": sport,
                 "dip": dip,
                 "dport": dport,
                 "protocol": protocol,
                 "detect_time": timestamp
        }
        logger.info(model)
        redis_conn.zadd("origin_data", {json.dumps(model): timestamp})
    # 执行表达式，生成标签
    message["expression_label"] = []

    return 'success', json.dumps(message)


'''
@description: 主程序
'''
def main():
    functions = {
                 "black_log_converted": convert_log,
                 "white_log_converted": convert_log
    }
    topics = ["white_log_converted", "black_log_converted"]  # 待消费话题集合
    #topics = functions.keys()
    processes = []
    logger.info('Parent process %s.' % os.getpid())
    # 启动多进程方式同时消费所有话题
    for eachtopic in topics:
        p = Process(target=threatsConsume, args=(eachtopic, functions, ))
        logger.info('Child process will start.')
        processes.append(p)
    for eachprocess in processes:
        eachprocess.start()
    for eachprocess in processes:
        eachprocess.join()
    logger.info('Child process end.')


if __name__ == "__main__":
    main()


