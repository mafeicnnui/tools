#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/5/6 11:52
# @Author : 马飞
# @File : modify_key.py
# @Software: PyCharm

import redis

settings={
     "db_sour":{
                "host":"r-2ze9f53dad8419b4.redis.rds.aliyuncs.com",
                "port":6379,
                "db":0,
                "password":'WXwk2018'
     },
     "key_match":"homePage:function:goods:218"
}

def get_config():
    cfg={}
    cfg['db_sour']   = redis.Redis(host=settings['db_sour']['host'],
                                   port=settings['db_sour']['port'],
                                   password=settings['db_sour']['password'],
                                   db=settings['db_sour']['db'])

    cfg['key_match'] = settings['key_match']
    return cfg

def get_keys(cfg):
    db=cfg['db_sour']
    key=[]
    for i in cfg['key_match'].split(','):
        tmp = db.keys(i)
        for j in tmp:
            key.append(j)
    return key

def main():

    cfg   = get_config()
    keys  = get_keys(cfg)
    print('cfg=',cfg)
    print('keys=',keys)

    keys.sort()
    for key in keys:
        print(key,cfg['db_sour'].get(key).decode('UTF-8'))

if __name__ == "__main__":
     main()
