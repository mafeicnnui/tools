#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : 马飞
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

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
    with open('/home/hopson/apps/usr/webserver/./dba_tools/slow_log/log/show_detail_ecs.log', 'r') as f:
        r = f.read()
    return r

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def write_db(p_cfg,d_log):
    db = p_cfg['db']
    cr = db.cursor()
    st = '''
           insert into t_slow_detail
               (sql_id,templete_id,finish_time,USER,HOST,ip,thread_id,query_time,lock_time,
                rows_sent,rows_examined,db,sql_text,finger,bytes,cmd,pos_in_log)
          values('{}','{}',STR_TO_DATE('{}', '%Y%m%d %H:%i:%s'),'{}','{}','{}','{}','{}','{}',
                 '{}','{}','{}','{}','{}','{}','{}','{}')
         '''.format(d_log['sql_id'],
                    d_log['templete_id'],
                    d_log['finish_time'],
                    d_log['user'],
                    d_log['host'],
                    d_log['ip'],
                    d_log['thread_id'],
                    d_log['query_time'],
                    d_log['lock_time'],
                    d_log['rows_sent'],
                    d_log['rows_examined'],
                    d_log['db'],
                    format_sql(d_log['sql_text']),
                    format_sql(d_log['finger']),
                    d_log['bytes'],
                    d_log['cmd'],
                    d_log['pos_in_log']
                    )
    #print(st)
    cr.execute(st)
    db.commit()
    p_cfg['row'] = p_cfg['row']+1
    print('\rinsert {} rows!'.format(p_cfg['row']),end='')

def parse_log_detail(p_cfg,p_log):
    d_log = {}
    rows  = p_log.split('\n')[1:-1]
    #print('rows1=', rows)
    d_log['lock_time']     = rows[0].split('=>')[1].replace("'",'').replace(',','').replace(' ','')
    d_log['query_time']    = rows[1].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['rows_examined'] = rows[2].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['rows_sent']     = rows[3].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['thread_id']     = rows[4].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['sql_text']      = re.sub(' +', ' ',''.join(rows[5:]).split(',  bytes')[0].split('arg =>')[1][2:-1].replace('\\',''))
    d_log['sql_id']        = get_md5(d_log['sql_text'])
    d_log['bytes']         = re.sub(' +', ' ',''.join(rows[5:]).split(',  bytes')[1]).split(',')[0].replace(' => ','')
    d_log['cmd']           = re.sub(' +', ' ',''.join(rows[5:]).split(',  cmd')[1]).split(',')[0].replace(' => ', '').replace("'","")
    d_log['db']            = re.sub(' +', ' ',''.join(rows[5:]).split(',  db')[1]).split(',')[0].replace(' => ', '').replace("'", "")
    d_log['finger']        = re.sub(' +', ' ',''.join(rows[5:]).split(',  host')[0].split('fingerprint =>')[1][2:-1].replace('\\',''))
    d_log['templete_id']   = get_md5(d_log['finger'])
    d_log['host']          = re.sub(' +', ' ',''.join(rows[5:]).split(',  host')[1]).split(',')[0].replace(' => ','').replace("'","")
    d_log['ip']            = re.sub(' +', ' ',''.join(rows[5:]).split(',  ip')[1]).split(',')[0].replace(' => ','').replace("'","")
    d_log['pos_in_log']    = re.sub(' +', ' ',''.join(rows[5:]).split(',  pos_in_log')[1]).split(',')[0].replace(' => ', '')
    d_log['finish_time']   = '20'+re.sub(' +', ' ',''.join(rows[5:]).split(',  ts')[1]).split(',')[0].replace(' => ', '').replace("'","")
    d_log['user']          = re.sub(' +', ' ',''.join(rows[5:]).split(',  user')[1]).split(',')[0].replace(' => ', '').replace("'","").replace('};','')
    #print(json.dumps(d_log, ensure_ascii=False, indent=4, separators=(',', ':')))
    write_db(p_cfg,d_log)

if __name__ == "__main__":
    cfg = {}
    cfg['db'] = get_db('10.2.39.18','3306','puppet','puppet','Puppet@123')
    cfg['row'] = 0
    for log in get_log().split('$VAR1 =')[1:]:
        #print('log=',log)
        parse_log_detail(cfg,log)