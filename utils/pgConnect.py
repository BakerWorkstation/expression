#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 13:45:48
@LastEditTime: 2020-03-20 18:27:56
@LastEditors: Please set LastEditors
@Description: redis共享连接池
@FilePath: /home/sdc/program-56/utils/redisConnect.py
'''

import redis
import psycopg2
from DBUtils.PooledDB import PooledDB


'''
@description:  初始化pg连接池对象
@param  conf(dict)
@return:  pg_conn(object)
'''
def pgpool(config_env): 
    try:
        # 开启pg连接池
        pg_pool = PooledDB(
                            psycopg2,
                            1,
                            database=config_env["pg_db"],
                            user=config_env["pg_user"],
                            password=config_env["pg_passwd"], 
                            host=config_env["pg_host"],
                            port=config_env["pg_port"]
        )
    except psycopg2.OperationalError as e:
        print("Error: threatsConsume function pg connect fail-> message: %s" % str(e))
        sys.exit(0)
    pg_conn = pg_pool.connection()
    return pg_conn
