#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 13:55:15
@LastEditTime: 2020-03-21 11:09:14
@LastEditors: Please set LastEditors
@Description: 加载配置文件
@FilePath: /home/sdc/program-56/config.py
'''


import os
import sys
server_dir = "/home/demo/dataCollect"
sys.path.append(os.path.join(server_dir, "./env"))
import redis
import configparser
from utils.log import record

'''
@description:  配置进程中的环境变量
'''
def _init():
    global _global_dict
    try:
        conf = configparser.ConfigParser()
        conf.read(os.path.join(server_dir, "../conf/system_conf.ini"))
        redis_ip = conf.get("redis", "REDIS_HOSTNAME").replace('"', '').strip()
        redis_port = conf.get("redis", "REDIS_PORT")
        redis_passwd = conf.get("redis", "REDIS_PASSWORD").replace('"', '').strip()
        pg_host = conf.get("postgres", "HOST").strip()
        pg_port = conf.get("postgres", "PORT")
        pg_db = conf.get("postgres", "DATABASE").strip()
        pg_user = conf.get("postgres", "USER").strip()
        pg_passwd = conf.get("postgres", "PASSWORD").strip()
        kafka_ip = conf.get("kafka", "IP").strip()
        kafka_port = int(conf.get("kafka", "PORT").strip())
        kafka_group_id = conf.get("kafka", "GROUP_ID").strip()
        session_timeout = int(conf.get("kafka", "TIMEOUT").strip())
        kafka_reset = conf.get("kafka", "RESET").strip()
        kafka_max_poll = int(conf.get("kafka", "MAX_POLL").strip())
        length = int(conf.get("kafka", "LENGTH").strip())
        kafka_partitions = int(conf.get("kafka", "PARTITIONS").strip())
        ftp_port = int(conf.get("ftp", "PORT").strip())
        ftp_user = conf.get("ftp", "USERNAME").strip()
        ftp_passwd = conf.get("ftp", "PASSWORD").strip()
        ftp_filedir = conf.get("ftp", "FILEDIR").strip()
        #redis_db = conf.get("REDIS", "db")
        redis_db = 2
        conf = configparser.ConfigParser()
        conf.read(os.path.join(server_dir, "conf/server.conf"))
        log_level = conf.get("LOG", "level")
        wait = int(conf.get("SLEEP", "wait"))

        _global_dict = {
                        "redis_ip": redis_ip,
                        "redis_port": int(redis_port),
                        "redis_passwd": redis_passwd,
                        "redis_db": redis_db,
                        "kafka_ip": kafka_ip,
                        "kafka_port": kafka_port,
                        "kafka_group_id": kafka_group_id,
                        "session_timeout": session_timeout,
                        "kafka_reset": kafka_reset,
                        "kafka_max_poll": kafka_max_poll,
                        "length": length,
                        "kafka_partitions": kafka_partitions,
                        "pg_host": pg_host,
                        "pg_port": int(pg_port),
                        "pg_db": pg_db,
                        "pg_user": pg_user,
                        "pg_passwd": pg_passwd,
                        "ftp_port": ftp_port,
                        "ftp_user": ftp_user,
                        "ftp_passwd": ftp_passwd,
                        "ftp_filedir": ftp_filedir,
                        "server_dir": server_dir,
                        "wait": wait
        }
    except Exception as e:
        print(str(e))
        sys.exit()

'''
@description:  返回进程中的环境变量
@return: defValue(dict)
'''
def get_value():
    try:
        return _global_dict
    except KeyError:
        return defValue

