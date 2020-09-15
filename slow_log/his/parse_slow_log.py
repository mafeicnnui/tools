#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/22 10:56
# @Author : 马飞
# @File : parse_slow_log.py
# @Software: PyCharm

import json
import re

log = '''
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

log2 = '''
$VAR1 = {
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
                          SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id)' 
};
'''

log3 = '''
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


def get_log():
    log=''
    with open('/home/hopson/apps/usr/webserver/./dba_tools/slow_log/show_detail_ecs.log', 'r') as f:
        log = f.read()
    return log

def write_log():
    pass

def parse(p_log):
    print('p_log=',p_log)
    if p_log!='\n':
        # v = p_log.replace('{','').replace('};','').replace('\n','').split(',  ')
        # print('v=',v)
        # print('len(v)=',len(v))
        # print('-----------------------------')
        # for i in v:
        #     print('i=',i)
        #     t = re.sub(' +', ' ', i)
        #     print('t=',t)
            # for k in t:
            #     print(k,ord(k))


        v = p_log.replace('{', '').replace('};', '').replace('\n', '')
        print('v1=',v)
        v= re.sub(' +', ' ', v)
        print('v2=', v)

        x={}
        for i in v.split("', "):
           #print('i=',i,i.split('=>'))
           if i.count('bytes =>')>0:
              x['bytes'] = i.split(',')[0].split('=>')[1]
              x['cmd']   = i.split(',')[1].split('=>')[1]

           elif i.count('pos_in_log =>')>0:
              x['pos_in_log'] = i.split(',')[0].split('=>')[1]
              x['timestamp']  = i.split(',')[1].split('=>')[1]

           else:
              print('>>>',i.split('=>'),len(i.split('=>')))
              key = i.split('=>')[0]
              # val = i.split('=>')[1]

              #  k =  i.split('=>')[0].replace(' ','')
          #  v =  re.sub(' +',' ',i.split('=>')[1])
          # d[i.split('=>')[0].replace(' ','')]=re.sub(' +',' ',i.split('=>')[1])
          # print(i,i.split('=>')[0],i.split('=>')[1])
        print(json.dumps(x, ensure_ascii=False, indent=4, separators=(',', ':')))

        # d = {}
        # for i in v.split("', "):
        #    print('i=', i)
        #    #d[i.split('=>')[0].replace(' ','')]=re.sub(' +',' ',i.split('=>')[1])
        #    if re.compile('bytes =>*').search(i) :
        #        d['bytes'] = i.split(',')[0].split('=>')[1]
        #        d['cmd']   = i.split(',')[1].split('=>')[1]
        #    elif re.compile('pos_in_log =>*').search(i):
        #        d['pos_in_log'] = i.split(',')[0].split('=>')[1]
        #        d['timestamp']  = i.split(',')[1].split('=>')[1]
        #    else:
        #       d[i.split('=>')[0].replace(' ', '')] = i.split('=>')[1]
        #    # print(i,i.split('=>')[0],i.split('=>')[1])
        #    # print(i.split('=>')[0],i.split('=>')[1])
        # print(json.dumps(d, ensure_ascii=False, indent=4, separators=(',', ':')))


        # for i in v:
        #     print(re.sub(' +', ' ', i))
        #     for j in re.sub(' +', ' ', i).split('=>'):
        #         print(j)

        #v =re.sub(' +',' ',v).split(',  ')
        #print('v2=', v)
        #print('v3=',v.split('=>')[0],v.split('=>')[1])

        # d = {}
        # for i in v:
        #    #d[i.split('=>')[0].replace(' ','')]=re.sub(' +',' ',i.split('=>')[1])
        #    #print(i,i.split('=>')[0],i.split('=>')[1])
        #    print(i.split('=>')[0],i.split('=>')[1])
        #print(json.dumps(d, ensure_ascii=False, indent=4, separators=(',', ':')))

if __name__ == "__main__":

    log=get_log()
    print('log=',log)
    for l in log.split('$VAR1 = '):
        print('l=',l)
        parse(l)

