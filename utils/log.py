#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc
'''

@Author: sdc
@Date: 2020-03-20 14:00:30
@LastEditTime: 2020-03-20 14:33:49
@LastEditors: Please set LastEditors
@Description: 输出日志格式化
@FilePath: /home/sdc/program-56/utils/log.py
'''


import os
import time
import logging
import datetime
from logging import handlers

def record(**kwargs):
    level = kwargs['level']
    datefmt = kwargs.pop('datefmt', None)
    format = kwargs.pop('format', None)
    server_dir = kwargs.pop('server_dir', None)
    if level is None:
        level = logging.INFO
    if datefmt is None:
        datefmt = '%Y-%m-%d %H:%M:%S'
    if format is None:
        format = '%(asctime)s [%(module)s] %(levelname)s [%(lineno)d] %(message)s'
    if server_dir is None:
        server_dir = "/var/log/"
    #else:
        #server_dir = os.path.join(server_dir, "logs")
    filename = os.path.join(server_dir, kwargs['filename'])
    log = logging.getLogger(filename)
    format_str = logging.Formatter(format, datefmt)
    th = handlers.TimedRotatingFileHandler(filename=filename, backupCount=7, when='midnight', encoding='utf-8')
    #th._namer = lambda x: 'test.' + x.split[-1]
    th.setFormatter(format_str)
    th.setLevel(level)
    log.addHandler(th)
    log.setLevel(level)
    return log
