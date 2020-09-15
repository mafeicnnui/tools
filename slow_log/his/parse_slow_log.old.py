#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/22 10:56
# @Author : 马飞
# @File : parse_slow_log.py
# @Software: PyCharm

import json
import re

log2 = '''
$VAR1 = {
  Lock_time => '3.932541',
  Query_time => '12.967225',
  Rows_examined => '0',
  Rows_sent => '0',
  Thread_id => '128962755',
  arg => 'insert into t_monitor_task_db_log (task_tag,server_id,db_id,total_connect,active_connect,db_available,create_date) 
                      values(\'gather_task_db_103\',\'43\',\'103\',\'9\',\'0\',\'1\',now())',
  bytes => 195,
  cmd => 'Query',
  db => 'puppet',
  fingerprint => 'insert into t_monitor_task_db_log (task_tag,server_id,db_id,total_connect,active_connect,db_available,create_date) values(?,?,?,?,?,?,now())',
  host => '10.2.39.18',
  ip => '10.2.39.18',
  pos_in_log => 2003,
  timestamp => '1595120971',
  ts => '200719  9:09:31',
  user => 'puppet'
};
$VAR1 = {
  Lock_time => '14.132540',
  Query_time => '23.167270',
  Rows_examined => '0',
  Rows_sent => '0',
  Thread_id => '128962666',
  arg => 'insert into t_monitor_task_db_log (task_tag,server_id,db_id,total_connect,active_connect,db_available,create_date) 
                      values(\'gather_task_db_118\',\'60\',\'118\',\'None\',\'None\',\'1\',now())',
  bytes => 201,
  cmd => 'Query',
  db => 'puppet',
  fingerprint => 'insert into t_monitor_task_db_log (task_tag,server_id,db_id,total_connect,active_connect,db_available,create_date) values(?,?,?,?,?,?,now())',
  host => '10.2.39.18',
  ip => '10.2.39.18',
  pos_in_log => 2385,
  timestamp => '1595120971',
  ts => '200719  9:09:31',
  user => 'puppet'
};
'''

log = '''
$VAR1 = {
  Lock_time => '0.000107',
  Query_time => '2.224911',
  Rows_examined => '1041108',
  Rows_sent => '41',
  Thread_id => '128962338',
  arg => 'SELECT 
                   a.server_id,
                   a.create_date,
                   mem_usage          AS index_value,
                   \'mem_usage\'        AS index_code,
                   \'内存使用率\'        AS index_name 
                FROM t_monitor_task_server_log a ,t_server b
                 WHERE a.server_id=b.id 
                     AND b.server_ip NOT LIKE \'10.2.39.%\' 
                     AND (a.server_id,a.create_date) IN( 
                          SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id)',
  bytes => 576,
  cmd => 'Query',
  db => 'puppet',
  fingerprint => 'select a.server_id, a.create_date, mem_usage as index_value, ? as index_code, ? as index_name from t_monitor_task_server_log a ,t_server b where a.server_
id=b.id and b.server_ip not like ? and (a.server_id,a.create_date) in( select a.server_id, max(a.create_date) from t_monitor_task_server_log a group by server_id)',
  host => '10.2.39.17',
  ip => '10.2.39.17',
  pos_in_log => 1238,
  timestamp => '1595120947',
  ts => '200719  9:09:07',
  user => 'sync'
};
'''

def write_log():
    pass

def parse(p_log):
    if p_log!='\n':
        v = p_log.replace('{','').replace('};','').replace('\n','').split(',  ')
        print(v)

        d = {}
        for i in v:
           d[i.split('=>')[0].replace(' ','')]=re.sub(' +',' ',i.split('=>')[1])
        print(json.dumps(d, ensure_ascii=False, indent=4, separators=(',', ':')))

if __name__ == "__main__":
    print('log=',log)
    for l in log.split('$VAR1 = '):
        print('l=',l)
        parse(l)

