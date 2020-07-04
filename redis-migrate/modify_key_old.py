#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/5/6 11:52
# @Author : 马飞
# @File : modify_key.py
# @Software: PyCharm

import redis

settings={
     "db_sour":{
                "host":"10.2.39.170",
                "port":6379,
                "db":0},
     "db_dest":{
                "host":"10.2.39.170",
                "port":6379,
                "db":6},
     "key_match":"point:*,pointCms:*"
}

def print_dict(r,config):
    print('-'.ljust(150,'-'))
    print(' '.ljust(3,' ')+"name".ljust(60,' ')+"type".ljust(10,' ')+'value'.ljust(80,' '))
    print('-'.ljust(150,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(60,' ')+r.type(key).decode('UTF-8').ljust(10,' '),config[key].ljust(80,' '))
    print('-'.ljust(150,'-'))
    print('合计:{0}'.format(str(len(config))))


def get_config():
    cfg={}
    cfg['db_sour']   = redis.Redis(host=settings['db_sour']['host'], port=settings['db_sour']['port'], db=settings['db_sour']['db'])
    cfg['db_dest']   = redis.Redis(host=settings['db_dest']['host'], port=settings['db_dest']['port'], db=settings['db_dest']['db'])
    cfg['key_match'] = settings['key_match']
    return cfg

def get_keys(cfg):
    db=cfg['db_sour']
    key=[]
    for i in cfg['key_match'].split(','):
        tmp = db.keys(i)
        key.append(tmp)
    print('get_keys=',key)
    return key

def main():

    cfg   = get_config()
    keys  = get_keys(cfg)
    print(type(keys))

    #r0       = redis.Redis(host='10.2.39.170', port=6379, db=0)
    #r6       = redis.Redis(host='10.2.39.170', port=6379, db=6)

    #r0_keys  = r0.keys('pointCms*')
    #r0_keys  = r0.keys('point:*')

    #r0       = redis.Redis(host='10.2.39.70', port=6379, db=0)
    #r6       = redis.Redis(host='10.2.39.70', port=6379, db=6)
    #r0_keys  = r0.keys('pointCms*')
    #r0_keys  = r0.keys('point:*')
    rkeys    = {}
    d_name   = {}
    d_type   = {}
    print(cfg)

    keys.sort()
    for key in keys:
        d=cfg['db_sour'].dump(key)
        print(key,d)
        cfg['db_dest'].delete(key)
        cfg['db_dest'].restore(key,0,d)

if __name__ == "__main__":
     main()
