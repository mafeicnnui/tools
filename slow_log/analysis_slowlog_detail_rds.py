#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 11:21
# @Author : 马飞
# @File : analysis_slowlog_detail_rds.py.py
# @Software: PyCharm
# url:https://www.cnblogs.com/luyucheng/p/6265873.html

import json
import re
import hashlib
import pymysql
import traceback

def get_db(ip,port,service ,user,password):
    try:
        conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',connect_timeout=3)
        return conn
    except  Exception as e:
        print('get_db exceptiion:' + traceback.format_exc())
        return None

def get_md5(url):
    """
    由于hash不处理unicode编码的字符串（python3默认字符串是unicode）
        所以这里判断是否字符串，如果是则进行转码
        初始化md5、将url进行加密、然后返回加密字串
    """
    if isinstance(url, str):
        url = url.encode("utf-8")
    md = hashlib.md5()
    md.update(url)
    return md.hexdigest()

def get_log():
    with open('/home/hopson/apps/usr/webserver/./dba_tools/slow_log/slow_detail_rds.log', 'r') as f:
        r = f.read()
    return r

def parse_log_detail(p_cfg,p_log):
    d_log = {}
    rows  = p_log.split('\n')

    # first row get Time
    d_log['exec_finish_time'] = '20'+rows[0][1:-1]

    # second row get User and Host
    d_log['user']             = rows[1].split('# User@Host:')[1].split('@')[0].split('[')[0][1:]
    d_log['host']             = rows[1].split('# User@Host:')[1].split('@')[1].split('[')[0][1:-1]

    # third row get thread_id
    d_log['thread_id']        = rows[2].split('# Thread_id:')[1][1:-1]

    # fourth row get Query_time,Lock_time,Rows_sent,Rows_examined
    d_log['query_time']       = rows[3].split('# Query_time:')[1].split(' ')[1]
    d_log['lock_time']        = rows[3].split('Lock_time:')[1].split(' ')[1]
    d_log['rows_sent']        = rows[3].split('Rows_sent:')[1].split(' ')[1]
    d_log['rows_examined']    = rows[3].split('Rows_examined:')[1][1:]

    # fifth row get db
    d_log['db']               = rows[4].split('use ')[1][0:-2]

    # sixth row get sql,bytes
    d_log['sql_text']         =  re.sub(' +', ' ', ''.join(rows[5:]).replace('\n',''))
    d_log['bytes']            =  len(re.sub(' +', ' ', ''.join(rows[5:]).replace('\n', '')).encode())
    d_log['sql_id']           =  get_md5(d_log['sql_text'])

    print('rows=',rows)
    print(json.dumps(d_log, ensure_ascii=False, indent=4, separators=(',', ':')))
    write_db(p_cfg,d_log)

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def write_db(p_cfg,d_log):
    db = p_cfg['db']
    cr = db.cursor()
    st = '''
           insert into t_slow_detail(sql_id,exec_finish_time,USER,HOST,thread_id,query_time,lock_time,rows_sent,rows_examined,db,sql_text,bytes)
             values('{}',
                    STR_TO_DATE('{}', '%Y%m%d %H:%i:%s'),
                    '{}',
                    '{}',
                     {},
                     {},{},{},{},
                    '{}','{}',{})
         '''.format(d_log['sql_id'],
                    d_log['exec_finish_time'],
                    d_log['user'],
                    d_log['host'],
                    d_log['thread_id'],
                    d_log['query_time'],
                    d_log['lock_time'],
                    d_log['rows_sent'],
                    d_log['rows_examined'],
                    d_log['db'],
                    format_sql(d_log['sql_text']),
                    d_log['bytes']
                    )
    print(st)
    cr.execute(st)
    db.commit()
    print('insert ok!')


if __name__ == "__main__":
    cfg = {}
    cfg['db'] = get_db('10.2.39.18','3306','puppet','puppet','Puppet@123')
    for log in get_log().split('# Time:')[1:]:
        parse_log_detail(cfg,log)
