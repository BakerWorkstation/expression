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

'''
@description:  初始化redis连接池对象
@param  conf(dict)
@return:  redis_conn(object)
'''
def redispool(config_env): 
    # 开启redis连接池
    redis_pool = redis.ConnectionPool(
                                        host=config_env["redis_ip"],
                                        port=config_env["redis_port"],
                                        db=config_env["redis_db"],
                                        password=config_env["redis_passwd"],
                                        decode_responses=True
    )
    redis_conn = redis.Redis(connection_pool=redis_pool)
    return redis_conn
