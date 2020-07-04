#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/6/8 11:10
# @Author : 马飞
# @File : webchat.py.py
# @Software: PyCharm
# @Function: EaseBase服务监控微信推送

import requests
import traceback
import pymysql
import datetime
import time
import json

'''
  功能：全局配置
'''
config = {
    "chat_interface":"https://alarm.lifeat.cn/wx/cp/msg/1000011",
    "mysql":"rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com:3306:information_schema:hopsononebi_2019:xMEAnLk8SOfa8bEwq2Xl6LpuI8QWXb0",
    # "mysql": "rm-2zer0v9g25bgu4rx4.mysql.rds.aliyuncs.com:3306:information_schema:hopsononebi_2019:xMEAnLk8SOfa8bEwq2Xl6LpuI8QWXb0",
    "warn_level":"紧急",
    "send_delay":300,
    "kill_threads_num":200
}

'''
  功能：获取mysql连接，以元组返回
'''
def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

'''
  功能：获取mysql连接，以字典返回
'''
def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

def str2datetime(p_rq):
    return datetime.datetime.strptime(p_rq, '%Y-%m-%d %H:%M:%S')


'''
 功能：获取数据库连接
'''
def get_config(config):
    db_ip                    = config['mysql'].split(':')[0]
    db_port                  = config['mysql'].split(':')[1]
    db_service               = config['mysql'].split(':')[2]
    db_user                  = config['mysql'].split(':')[3]
    db_pass                  = config['mysql'].split(':')[4]
    config['db_mysql_dict']  = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
    config['db_mysql_dict2'] = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
    get_checkpoint(config)
    return config

'''
  功能：调用接口发送消息
'''
def send_message(config,message):
    try:
        r = requests.post(config['chat_interface'], data=bytes(message, 'UTF-8'))
        print(r.text)
    except:
        print(traceback.print_exc())


'''
 功能：获取异常进程信息
'''
def start(config,n_threshold):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = """SELECT 
                USER,
                SUBSTR(HOST,1,INSTR(HOST,':')-1) AS HOST,
                COUNT(0) AS num,
                (SELECT COUNT(0) FROM information_schema.`PROCESSLIST` ) AS total_num,
                (SELECT COUNT(0) FROM information_schema.`PROCESSLIST` 
                 WHERE  HOST LIKE '192.168.100.%' 
                    AND USER='hopsononebi_ro'
                         AND command='Sleep') AS tatal2
                FROM information_schema.`PROCESSLIST` 
                 WHERE  id  NOT IN(SELECT trx_mysql_thread_id FROM information_schema.innodb_trx)
                  AND (HOST LIKE '192.168.100.%' OR HOST LIKE '124.127.103.190%' )
                  AND USER='hopsononebi_ro'
                  AND command='Sleep'
                GROUP BY USER,SUBSTR(HOST,1,INSTR(HOST,':')-1)
                HAVING COUNT(0)>{}""".format(n_threshold)
    cr.execute(st)
    rs=cr.fetchall()
    for r in rs:
        kill_session(config,r)
        if get_seconds(str2datetime(config['last_send_time']))>config['send_delay']:
           send_webchat(r)
           write_checkpoint()
    return rs


'''
 功能：杀sleep进程
'''
def kill_session(config,r):
    db = config['db_mysql_dict2']
    start_time = datetime.datetime.now()
    cr = db.cursor()
    st = """SELECT id 
            FROM information_schema.`PROCESSLIST` 
             WHERE  id  NOT IN(SELECT trx_mysql_thread_id FROM information_schema.innodb_trx)
              AND HOST like '{}%'
              AND USER = '{}'
              AND command='Sleep'
         """.format(r['HOST'],r['USER'])
    print('kill_session=>sql=',st)
    cr.execute(st)
    rs=cr.fetchall()
    print('Starting kill session...')
    for r in rs:
        try:
           cr.execute('kill {}'.format(r['id']))
           print('Thread {} killed!'.format(r['id']))
        except:
           pass
    print('Complete kill session...elaspse time:{}s'.format(str(get_seconds(start_time))))


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def str2datetime(p_rq):
    return datetime.datetime.strptime(p_rq, '%Y-%m-%d %H:%M:%S')

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())
'''
  功能：写检查点，最近发送微信通知的时间至文件
'''
def write_checkpoint():
    checkpoint = {}
    checkpoint['last_send_time']  = get_time()
    with open('/tmp/killSession_checkpoint.ini', 'w') as f:
        f.write(json.dumps(checkpoint, ensure_ascii=False, indent=4, separators=(',', ':')))


def get_checkpoint(config):
    try:
        with open('/tmp/killSession_checkpoint.ini', 'r') as f:
            tmp = f.read()
        checkpoint=json.loads(tmp)
        config['last_send_time'] = checkpoint['last_send_time']
    except:
      print(traceback.format_exc())
      print('write checkpoint first!')
      write_checkpoint()


def send_webchat(r):
    print('send_webchat=',r)
    ft = '''
用户名称  ：{}
连接地址  ：{}
总连接数  ：{}
睡眠连接数：{}
睡眠连数占比：{}
192.168.100.%连接数：{}
192.168.100.%连接占比：{}
发送时间：{}
'''
    v_title   = '[合生通]生产BI连接数异常批量杀进程通知'
    v_content = ft.format(
        r['USER'],
        r['HOST'],
        r['total_num'],
        r['num'],
        str(round(r['num'] / r['total_num'], 4) * 100) + '%',
        r['tatal2'],
        str(round(r['tatal2']/r['total_num'],4)*100)+'%',
        get_time()
    )
    send_message(cfg, v_title + v_content)



def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

if __name__ == "__main__":
    cfg = get_config(config)
    print_dict(cfg)
    start(cfg,config['kill_threads_num'])