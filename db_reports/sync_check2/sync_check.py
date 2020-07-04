#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Func : 停车统计检测情况表
# @Software: PyCharm
import sys,time
import traceback
import configparser
import warnings
import pymysql
import datetime
import smtplib
import os
from email.mime.text import MIMEText

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
        print('mail send success!')
    except smtplib.SMTPException as e:
        print('mail send failure!')
        print(e)

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_create_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:0:0")

def get_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def get_db(dbstr):
    db_ip      = dbstr.split(':')[0]
    db_port    = dbstr.split(':')[1]
    db_service = dbstr.split(':')[2]
    db_user    = dbstr.split(':')[3]
    db_pass    = dbstr.split(':')[4]
    conn = pymysql.connect(host=db_ip, port=int(db_port), user=db_user, passwd=db_pass, db=db_service, charset='utf8')
    return conn

def get_market_name(p_market_id):
    if p_market_id == '108':
        return '成都珠江广场'
    elif p_market_id=='110':
        return '上海五角场'
    elif p_market_id == '132':
        return '广州嘉和南'
    elif p_market_id == '141':
        return '北京合生财富广场'
    elif p_market_id == '145':
        return '广州珠江投资大厦'
    elif p_market_id == '150':
        return '广州合生骏景广场'
    elif p_market_id =='164':
        return '北京合生广场'
    elif p_market_id == '170':
        return '广州嘉和北'
    elif p_market_id == '188':
        return '广州越华珠江广场'
    elif p_market_id == '194':
        return '广州南方花园'
    elif p_market_id == '213':
        return '北京望金集团'
    elif p_market_id == '218':
        return '北京朝阳合生汇'
    elif p_market_id == '234':
        return '上海青浦米格'
    elif p_market_id=='20229':
        return '广州珠江国际纺织城'
    elif p_market_id=='10230':
        return '西安时代广场'
    else:
        return ''

def get_sync_status(config,p_market_id,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT COUNT(0)
             FROM {0}.tc_record_cleaning 
             WHERE market_id='{1}' 
               and optdate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
               and optdate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0]==0:
        return '×'
    sql = '''SELECT COUNT(0)
                FROM {0}.tc_record_cleaning 
             WHERE market_id='{1}'
               and optdate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
               and optdate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0] == 0:
       return '×'
    db.commit()
    cr.close()
    return '√'


def get_tjd_intime_rq(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(datein) FROM {0}.dbo_park_carlog'.format(p_market_db)
    print(sql)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_intime_cnt_old(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT count(0) 
              FROM {0}.dbo_park_carlog 
            where  datein>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
               and datein<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)
    print(sql)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_intime_cnt(config,p_market_db):
    n_tjd_intime        = get_tjd_intime_cnt_old(config,p_market_db)
    n_tjd_outtime       = get_tjd_outtime(config,p_market_db)
    n_tjd_spetime       = get_tjd_spetime(config,p_market_db)
    n_tjd_nooutpay_time = get_tjd_nooutpay_time(config, p_market_db)
    n_tjd_outpay_time   = get_tjd_outpay_time(config, p_market_db)
    n_tjd_spepay_time   = get_tjd_spepay_time(config, p_market_db)

    if n_tjd_intime==0  or n_tjd_intime<500 \
        or n_tjd_outtime==0 or n_tjd_outtime<500 \
          or n_tjd_spetime==0 or n_tjd_spetime<500 \
            or n_tjd_nooutpay_time== 0 or n_tjd_nooutpay_time<500 \
              or n_tjd_outpay_time == 0 or n_tjd_outpay_time<500 \
                or n_tjd_spepay_time == 0 or n_tjd_spepay_time<500 :
        return '''入场车辆日志表:{0}</br>出场车辆日志表:{1}</br>特殊放行日志表:{2}</br>未出场车辆支付日志表:{3}</br>出场车辆支付日志表:{4}</br>特殊放行车辆支付日志表:{5}
               '''.format(n_tjd_intime,n_tjd_outtime,n_tjd_spetime,
                          n_tjd_nooutpay_time,n_tjd_outpay_time,n_tjd_spepay_time)
    return get_tjd_intime_cnt_old(config,p_market_db)


def get_tjd_outtime(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT count(0) 
             FROM {0}.dbo_park_caroutlog
             where  dateout>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
                and dateout<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_spetime(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT count(0) 
                 FROM {0}.dbo_oper_datalog
                 where  createdate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
                    and createdate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_nooutpay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT count(0) 
                 FROM {0}.dbo_park_paylog
                 where  paydate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
                    and paydate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_outpay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT count(0) FROM {0}.dbo_park_payoutlog'.format(p_market_db)
    sql = '''SELECT count(0) 
              FROM {0}.dbo_park_payoutlog
             where  paydate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
                and paydate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_spepay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT count(0) 
             FROM {0}.dbo_park_specicarlog
            where  createdate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
               and createdate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY)
          '''.format(p_market_db)

    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_sync_tjd_status(config,p_market_db):
    if get_tjd_intime_cnt_old(config,p_market_db)==0  or get_tjd_intime_cnt_old(config,p_market_db)<500 \
        or get_tjd_outtime(config,p_market_db)==0 or get_tjd_outtime(config,p_market_db)<500 \
          or get_tjd_spetime(config,p_market_db)==0 or get_tjd_spetime(config,p_market_db)<500 \
            or get_tjd_nooutpay_time(config, p_market_db) == 0 or get_tjd_nooutpay_time(config,p_market_db)<500 \
              or get_tjd_outpay_time(config, p_market_db) == 0 or get_tjd_outpay_time(config,p_market_db)<500 \
                or get_tjd_spepay_time(config, p_market_db) == 0 or get_tjd_spepay_time(config,p_market_db)<500 :
        return '×'
    return '√'


def get_html_contents(config):
    cr     = config['db_bi'].cursor()
    v_html = '''<html>
                    <head>
                       <style type="text/css">
                           .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                           .xwtable thead td {font-size: 12px;color: #333333;
                                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable thead th {font-size: 12px;color: #333333;
                                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                           .xwtable tbody tr.alt-row {background: #f2f7fc;}
                           .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                       </style>
                    </head>
                    <body>
                       <h3 align="center"><b>停车统计同步任务检测情况表</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$TABLE$$
                       <p></p>
                       <h3>说明：</h3>
                        <ul>
                           <li> 1.各项目车流T-1日统计检测</li> 
                           <li> 2.每天17:00定时调用</li> 
                        </ul>
                    </body>
                  </html>
               '''
    #捷顺实时车流
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '<thead>{0}</thead>'
    v_tab_tbody   = '<tbody>'
    v_sql  = '''SELECT 
                      concat(t.market_id,'') AS '项目编码',
                      ''                     AS '项目名称',
                      t.db                   AS '项目数据库',
                      t.optdate              AS '操作时间',
                      t.amount               AS 'T-1天数量',
                      t.`state`              AS '同步状态'
                FROM sync.t_sync_park_stats t
                WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_park_stats)
             '''

    cr.execute(v_sql)
    rs    = cr.fetchall()
    desc  = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
    v_row=v_row+'</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[5]=='×':
            v_tab_tbody = v_tab_tbody + \
                '''<tr>
                   <td><font color="red">{0}</font></td>
                   <td><font color="red">{1}</font></td>
                   <td><font color="red">{2}</font></td>
                   <td><font color="red">{3}</font></td>
                   <td><font color="red">{4}</font></td>
                   <td><font color="red">{5}</font></td>
                 </tr>  
                '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody +\
                '''<tr>
                     <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td>
                   </tr>  
                '''.format(i[0],get_market_name(i[0]),i[2],i[3],i[4],i[5])
            v_tab_tbody=v_tab_tbody+'</tbody>'
    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html=v_html.replace('$$TABLE$$',v_table)
    v_html=v_html.replace('$$TJRQ$$',get_date())
    cr.close()
    print(v_html)
    return v_html

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    config['db_bi']         = get_db(cfg.get("sync","db_bi"))
    config['market_id']     = cfg.get("sync", "market_id")
    config['market_id_tjd'] = cfg.get("sync", "market_id_tjd")
    config['send_user']     = cfg.get("sync", "send_mail_user")
    config['send_pass']     = cfg.get("sync", "send_mail_pass")
    config['acpt_user']     = cfg.get("sync", "acpt_mail_user")
    config['mail_title']    = cfg.get("sync", "mail_title")
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def format_table(sync_tab):
    v_line     = ''
    v_lines    = ''
    i_counter  = 0
    v_space    = ' '.ljust(25,' ')
    for tab in  sync_tab.split(","):
        v_line    = v_line+tab+','
        i_counter = i_counter+1
        if i_counter%5==0:
           if i_counter==5:
              v_lines = v_lines + v_line[0:-1] + '\n'
              v_line  = ''
           else:
              v_lines = v_lines +v_space+ v_line[0:-1] + '\n'
              v_line  = ''
    v_lines = v_lines + v_space+v_line[0:-1]
    return v_lines

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        if key=='sync_table':
           print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', format_table(config[key]))
        else:
           print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def get_tjrq():
    return (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')

def write_stats(config):
    db  = config['db_bi']
    cr  = db.cursor()
    rq  = get_create_time()
    cr.execute("delete from sync.t_sync_park_stats where create_date='{0}'".format(rq))
    for p in config['market_id'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_status    = get_sync_status(config,v_market_id,v_market_db)
        v_stats_sql ='''insert into sync.t_sync_park_stats
                           (market_id,db,optdate,amount,state,create_date)
                        SELECT
                           MAX(market_id)  AS market_id,
                           '{0}'           AS market_db,
                           MAX(optdate)    AS optdate,
                           (select count(0) 
                             from {1}.tc_record_cleaning t
                             where market_id='{2}'
                               and t.optdate>=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),INTERVAL -1 DAY) 
                               and t.optdate<=DATE_ADD(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 23:59:59'),INTERVAL -1 DAY) )   as amount,
                           '{3}'   AS state,        
                           '{4}'   AS create_time
                         FROM {5}.tc_record_cleaning t
                        WHERE t.market_id='{6}'
                     '''.format(v_market_db,v_market_db,v_market_id,v_status,rq,v_market_db,v_market_id)
        print(v_stats_sql)
        cr.execute(v_stats_sql)
        print('{0} write_stats insert complete'.format(v_market_db))
    db.commit()

def write_stats_tjd(config):
    db  = config['db_bi']
    cr  = db.cursor()
    rq  = get_create_time()
    #cr.execute("delete from sync.t_sync_park_stats where create_date='{0}'".format(rq))
    for p in config['market_id_tjd'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_status    = get_sync_tjd_status(config, v_market_db)
        v_intime    = get_tjd_intime_rq(config, v_market_db)
        v_incnt     = get_tjd_intime_cnt(config, v_market_db)
        v_create    = get_create_time()
        v_stats_sql ='''insert into sync.t_sync_park_stats
                           (market_id,db,optdate,amount,state,create_date)
                        values('{0}','{1}','{2}','{3}','{4}','{5}')
                     '''.format(v_market_id,v_market_db,v_intime,v_incnt,v_status,rq)
        print(v_stats_sql)
        cr.execute(v_stats_sql)
        print('{0} write_stats_tjd insert complete'.format(v_market_db))
    db.commit()

def init(config):
    config = get_config(config)
    print_dict(config)
    return config

def main():
    #init variable
    config = ""
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]

    #init config
    config=init(config)

    #write status
    write_stats(config)
    write_stats_tjd(config)
    #send mail
    send_mail465(config['send_user'], config['send_pass'],
                   config['acpt_user'], config['mail_title'],
                   get_html_contents(config))



if __name__ == "__main__":
     main()
