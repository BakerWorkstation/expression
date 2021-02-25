#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__: sdc

'''
@Author: sdc
@Date: 2020-03-20 15:23:23
@LastEditTime: 2020-03-20 20:28:45
@LastEditors: Please set LastEditors
@Description: 构造模拟数据，生成相应的json数据文件
@FilePath: /home/sdc/program-56/utils/createJson.py
'''


import uuid
import json
import copy
import random


'''
@description: 模拟icmp 扫描全网段行为  
@param {type} 
@return: 
'''
def icmp():
    dataList = []
    datadict = {
                "id": None,
                "sip": "10.255.10.10",
                "dip": "10.255.175.1",
                "sport": "",
                "dport": "",
                "proto": "icmp",
                "direct": "up",
                "url": "",
                "file": [
                         {
                          "name": "",
                          "type": "",
                          "md5": "",
                          "size": "",
                          "suffix": ""
                         }
                ],
                "label": ["executable_file", "Untrusted_address", "Not_detected", "No_trusted_signature", "uplink", "Encrypted_or_unrecognized", "HTTP", "FTP", "Descending"],
                "reference": {}
    }
    with open('models/scan_ips.json', 'w') as f:
        for num in range(1, 255):
            datadict["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
            datadict["dip"] = "10.255.175.%s" % num
            copydict = copy.deepcopy(datadict)
            dataList.append(copydict)
        json.dump({"data": dataList}, f, sort_keys=True, indent=4, separators=(',', ': '))

'''
@description: 模拟扫描主机所有端口行为  
@param {type} 
@return: 
'''
def ports():
    dataList = []
    datadict = {
                "id": None,
                "sip": "10.255.52.58",
                "dip": "10.255.175.110",
                "sport": "11111",
                "dport": "",
                "proto": "tcp",
                "direct": "up",
                "url": "",
                "file": [
                         {
                          "name": "",
                          "type": "",
                          "md5": "",
                          "size": "",
                          "suffix": ""
                         }
                ],
                "label": ["executable_file", "Untrusted_address", "Not_detected", "No_trusted_signature", "uplink", "Encrypted_or_unrecognized", "HTTP", "FTP", "Descending"],
                "reference": {}
    }
    with open('models/scan_ports.json', 'w') as f:
        for num in range(1, 255):
            datadict["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
            datadict["dport"] = "%s" % random.randint(1024, 49152) # 动态端口(1024 - 49151)
            copydict = copy.deepcopy(datadict)
            dataList.append(copydict)
        json.dump({"data": dataList}, f, sort_keys=True, indent=4, separators=(',', ': '))

'''
@description: 模拟ddos行为  
@param {type} 
@return: 
'''
def ddos():
    dataList = []
    datadict = {
                "id": None,
                "sip": "10.255.52.100",
                "dip": "10.255.49.17",
                "sport": "",
                "dport": "80",
                "proto": "http",
                "direct": "up",
                "url": "",
                "file": [
                         {
                          "name": "",
                          "type": "",
                          "md5": "",
                          "size": "",
                          "suffix": ""
                         }
                ],
                "label": ["executable_file", "Untrusted_address", "Not_detected", "No_trusted_signature", "uplink", "Encrypted_or_unrecognized", "HTTP", "FTP", "Descending"],
                "reference": {}
    }
    with open('models/ddos.json', 'w') as f:
        for num in range(1, 255):
            datadict["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
            datadict["sport"] = "%s" % random.randint(1024, 49152) # 动态端口(1024 - 49151)
            copydict = copy.deepcopy(datadict)
            dataList.append(copydict)
        json.dump({"data": dataList}, f, sort_keys=True, indent=4, separators=(',', ': '))

def main():
    icmp()
    ddos()
    ports()

if __name__ == "__main__":
    main()

