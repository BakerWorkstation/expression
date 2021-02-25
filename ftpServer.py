#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc


'''
@Author: sdc
@Date: 2020-04-21 09:23:13
@LastEditTime: 2020-04-21 10:11:56
@LastEditors: Please set LastEditors
@Description: FTP server
@FilePath: /opt/ftpServer/bin/test.py
'''

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

server_dir = config_env["server_dir"]

import os
import sys
import time
import shutil
import logging
from hashlib import md5
sys.path.append(os.path.join(server_dir, 'env'))
from utils.log import record
from pyftpdlib.servers import FTPServer
from pyftpdlib.handlers import TLS_FTPHandler
from pyftpdlib.authorizers import DummyAuthorizer, AuthenticationFailed

logger = record(filename='ftp_server.log',
                server_dir=os.path.join(server_dir, "logs/"),
                level=logging.INFO)

elogger = record(filename='ftp_error.log',
                server_dir=os.path.join(server_dir, "logs/"),
                level=logging.ERROR)

ftp_port = config_env["ftp_port"]
ftp_user = config_env["ftp_user"]
ftp_passwd = config_env["ftp_passwd"]
ftp_filedir = config_env["ftp_filedir"]

# 获取文件的md5值
def get_hexdigest(filename):
    hashobj = md5()
    with open(filename, 'rb') as f:        
        hashobj.update(f.read())
    md5_str = hashobj.hexdigest().lower()
    return md5_str

class DummyMD5Authorizer(DummyAuthorizer):

    def validate_authentication(self, username, password, handler):
        password = md5(password.encode('utf-8'))
        hash = password.hexdigest()
        try:
            if self.user_table[username]['pwd'] != hash:
                raise KeyError
        except KeyError:
            raise AuthenticationFailed

class MyHandler(TLS_FTPHandler):

    def on_connect(self):
        logger.info("%s:%s connected" % (self.remote_ip, self.remote_port))

    def on_disconnect(self):
        # do something when client disconnects
        pass

    def on_login(self, username):
        # do something when user login
        pass

    def on_logout(self, username):
        # do something when user logs out
        pass

    def on_file_sent(self, file):
        # do something when a file has been sent
        logger.info(self.username, file)

    def on_file_received(self, file):
        # do something when a file has been received
        logger.info(file)
        filename = file.split('/')[-1]

        # 过滤文件哈希库
        file_md5 = get_hexdigest(file)
        logger.info(file_md5)

        # 拷贝到yara工作目录
        shutil.copyfile(file, os.path.join(ftp_filedir, filename))
        # 清理文件存储目录
        os.remove(file)


    def on_incomplete_file_sent(self, file):
        # do something when a file is partially sent
        logger.info(self.username, file)

    def on_incomplete_file_received(self, file):
        # remove partially uploaded files
        logger.info(time.time())
        import os
        os.remove(file)

def main():
    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = DummyMD5Authorizer()

    # Define a new user having full r/w permissions and a read-only
    # anonymous user
    hash_t = md5(ftp_passwd).hexdigest()
    authorizer.add_user(ftp_user, hash_t, '/dev/shm/', perm='elradfmwMT')
    # Instantiate FTP handler class
    handler = MyHandler

    handler.certfile = os.path.join(server_dir,'./ca/server.crt')
    handler.keyfile = os.path.join(server_dir, './ca/server.key')
    handler.authorizer = authorizer
    #   logging.basicConfig(filename='/home/mhp/ftp_test/pyftpdlib/log/pyftp.log',level=logging.INFO)
    # Define a customized banner (string returned when client connects)
    handler.banner = "pyftpdlib based ftpd ready."

    # Specify a masquerade address and the range of ports to use for
    # passive connections.  Decomment in case you're behind a NAT.
    #handler.masquerade_address = '151.25.42.11'
    handler.passive_ports = range(40000, 65535)

    # Instantiate FTP server class and listen on 0.0.0.0:2121
    address = ('0.0.0.0', ftp_port)
    server = FTPServer(address, handler)

    # set a limit for connections
    server.max_cons = 256
    server.max_cons_per_ip = 5

    # start ftp server
    server.serve_forever()

if __name__ == '__main__':
    main()
