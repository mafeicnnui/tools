#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Func : 实时车流同步任务检测程序
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
    elif p_market_id == '155':
        return '深圳珠江广场'
    elif p_market_id == '159':
        return '创意合金工厂'
    elif p_market_id =='164':
        return '北京合生广场'
    elif p_market_id == '170':
        return '广州嘉和北'
    elif p_market_id == '177':
        return '合创产业中心科研'
    elif p_market_id == '183':
        return '上海合生财富广场'
    elif p_market_id == '188':
        return '广州越华珠江广场'
    elif p_market_id == '194':
        return '广州南方花园'
    elif p_market_id == '203':
        return '科华数码广场有限公司'
    elif p_market_id == '207':
        return '北京德胜'
    elif p_market_id == '213':
        return '合生新天地'
    elif p_market_id == '218':
        return '北京朝阳合生汇'
    elif p_market_id == '234':
        return '上海青浦米格'
    elif p_market_id=='20229':
        return '广州珠江国际纺织城'
    elif p_market_id=='10230':
        return '西安时代广场'
    elif p_market_id=='237':
        return '广州增城合生汇'
    else:
        return ''


def get_real_flow_status(config,p_market_id,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT COUNT(0)
             FROM {0}.dbo_summary_thirty WHERE market_id='{1}' AND countdate>DATE_ADD(NOW(),INTERVAL -1 HOUR)
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0]==0:
        return '×'
    db.commit()
    cr.close()
    return '√'

def get_flow_status(config,p_market_id,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT COUNT(0)
             FROM {0}.dbo_summary_day WHERE market_id='{1}' AND countdate>DATE_ADD(NOW(),INTERVAL -2 DAY)
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0]==0:
        return '×'
    db.commit()
    cr.close()
    return '√'

def get_sync_status(config,p_market_id,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT COUNT(0)
             FROM {0}.tc_record_cleaning WHERE market_id='{1}' AND optdate>DATE(NOW())
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0]==0:
        return '×'
    sql = '''SELECT COUNT(0)
                FROM {0}.tc_record_cleaning WHERE market_id='{1}' AND optdate>DATE_ADD(NOW(),INTERVAL -30 MINUTE)
               '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0] == 0:
       return '×'
    db.commit()
    cr.close()
    return '√'

def get_sync_status_biz(config,p_market_id,p_market_db):
    db = config['db_biz']
    cr = db.cursor()
    sql = '''SELECT COUNT(0)
             FROM {0}.dbo_userwork WHERE market_id='{1}' AND optdate>DATE(NOW())
          '''.format(p_market_db,p_market_id)
    cr.execute(sql)
    rs = cr.fetchone()
    if rs[0]==0:
        return '×'
    db.commit()
    cr.close()
    return '√'

def get_max_sync_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(create_date) FROM sync.t_sync_real_park_stats'
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_max_flow_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT MAX(create_date) FROM sync.t_sync_flow_stats 
             where db in('hopsonone_flow_bj','hopsonone_flow_sh','hopsonone_flow_cd','hopsonone_flow_gz')
          '''
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_max_real_flow_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = '''SELECT MAX(create_date) FROM sync.t_sync_flow_stats 
               where db in('hopsonone_flow_bj_real_time','hopsonone_flow_sh_real_time',
                           'hopsonone_flow_cd_real_time','hopsonone_flow_gz_real_time')
          '''
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_max_sales_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(create_date) FROM sync.t_sync_sales_stats'
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_max_member_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(create_date) FROM sync.t_sync_member_stats'
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_max_sync_tjd_rq(config):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(create_date) FROM sync.t_sync_real_tjd_stats'
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]


def get_templete():
    return '''<html>
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
                       <h3 align="center"><b>捷顺实时车流数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$TABLE$$
                       
                       <p></p>
                       <h3 align="center"><b>停简单实时车流数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$TJD$$
                       
                       <p></p>
                       <h3 align="center"><b>收费员结算数据同步任务检测情况表[BIZ]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$BIZ$$
                       
                       <p></p>
                       <h3 align="center"><b>离线客流数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$FLOW$$
                       
                       <p></p>
                       <h3 align="center"><b>实时客流数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$REALF$$
                       
                       <p></p>
                       <h3 align="center"><b>销售数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$SALES$$
                       
                       <p></p>
                       <h3 align="center"><b>会员数据同步任务检测情况表[BI]</b></h3>
                       <p></p>
                       <h2>统计日期：<span>$$TJRQ$$</span></h2>
                       $$MEMBERS$$
                       
                       <h3>说明：</h3>
                        <ul>
                           <li> 1.各项目实时车流数据同步任务检测</li> 
                           <li> 2.每小时整点定时调用</li> 
                        </ul>
                    </body>
                  </html>
               '''

def get_html_contents(config):
    cr     = config['db_bi'].cursor()
    cr_biz = config['db_biz'].cursor()
    v_html = get_templete()

    #捷顺实时车流
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '<thead>{0}</thead>'
    v_tab_tbody   = '<tbody>'
    v_sql  = '''SELECT 
                      concat(t.market_id,'') AS '项目编码',
                      ''                     AS '项目名称',
                      t.db                   AS '数据库名',
                      t.intime               AS '最近入场时间',
                      t.outtime              AS '最近出场时间',
                      t.chargetime           AS '最近支付时间',
                      t.optdate              AS '最近操作时间',
                      t.amount_today         AS '当天同步记灵',
                      t.amount_half_hour     AS '半小时同步记录',
                      t.`state`              AS '同步状态'
                FROM sync.t_sync_real_park_stats t
                WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_real_park_stats)
             '''

    cr.execute(v_sql)
    rs    = cr.fetchall()
    desc  = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        elif k in(7,8,9):
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
    v_row=v_row+'</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[9]=='×':
            v_tab_tbody = v_tab_tbody + \
                '''<tr>
                   <td><font color="red">{0}</font></td>
                   <td><font color="red">{1}</font></td>
                   <td><font color="red">{2}</font></td>
                   <td><font color="red">{3}</font></td>
                   <td><font color="red">{4}</font></td>
                   <td><font color="red">{5}</font></td>
                   <td><font color="red">{6}</font></td>
                   <td><font color="red">{7}</font></td>
                   <td><font color="red">{8}</font></td>
                   <td><font color="red">{9}</font></td>
                 </tr>  
                '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:
            v_tab_tbody = v_tab_tbody +\
                '''<tr>
                     <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td>
                     <td>{5}</td><td>{6}</td><td>{7}</td><td>{8}</td><td>{9}</td>
                   </tr>  
                '''.format(i[0],get_market_name(i[0]),i[2],i[3],i[4],i[5],i[6],i[7],i[8], i[9])
            v_tab_tbody=v_tab_tbody+'</tbody>'
    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html=v_html.replace('$$TABLE$$',v_table)
    v_html=v_html.replace('$$TJRQ$$',get_time())

    #停简单实时车流
    v_tab_header = '<table class="xwtable">'
    v_tab_thead  = '<thead>{0}</thead>'
    v_tab_tbody  = '<tbody>'
    v_sql = '''SELECT 
                         concat(t.market_id,'')   AS '项目编码',
                         ''                       AS '项目名称',
                         t.db                     AS '数据库名',
                         t.intime                 AS '最近入场时间',
                         t.outtime                AS '最近出场时间',
                         t.spetime                AS '最近特殊放行时间',
                         t.nooutpay_time          AS '最近未出场支付时间',
                         t.outpay_time            AS '最近出场支付时间',
                         t.spepay_time            AS '最近特殊放行支付时间'
                   FROM sync.t_sync_real_tjd_stats t
                   WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_real_tjd_stats)
            '''
    cr.execute(v_sql)
    rs = cr.fetchall()
    desc = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        v_tab_tbody = v_tab_tbody + \
                      '''<tr>
                            <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                            <td>{4}</td><td>{5}</td><td>{6}</td><td>{7}</td><td>{8}</td>
                         </tr>  
                      '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6],i[7],i[8])
        v_tab_tbody = v_tab_tbody + '</tbody>'
    v_tjd = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$TJD$$', v_tjd)
    #cr.close()

    #收费员结算
    v_tab_header = '<table class="xwtable">'
    v_tab_thead  = '<thead>{0}</thead>'
    v_tab_tbody  = '<tbody>'
    v_sql = '''SELECT 
                  concat(t.market_id,'') AS '项目编码',
                  ''                     AS '项目名称',
                  t.db                   AS '数据库名',
                  'dbo_userwork'         as '表名',
                  t.optdate              AS '最近操作时间',
                  t.amount_today         AS '当天同步记录',
                  t.amount_half_hour     AS '半小时同步记录',
                  t.`state`              AS '同步状态'
               FROM sync.t_sync_park_stats t
               WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_park_stats)
            '''
    cr_biz.execute(v_sql)
    rs    = cr_biz.fetchall()
    desc  = cr_biz.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'

    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[7] == '×':
            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                                 <td><font color="red">{0}</font></td>
                                 <td><font color="red">{1}</font></td>
                                 <td><font color="red">{2}</font></td>
                                 <td><font color="red">{3}</font></td>
                                 <td><font color="red">{4}</font></td>
                                 <td><font color="red">{5}</font></td>
                                 <td><font color="red">{6}</font></td>
                                 <td><font color="red">{7}</font></td>
                           </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4],i[5],i[6],i[7])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                               <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                               <td>{4}</td><td>{5}</td><td>{6}</td><td>{7}</td>
                             </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6], i[7])
            v_tab_tbody = v_tab_tbody + '</tbody>'
    v_biz  = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$BIZ$$', v_biz)

    #离线客流
    v_tab_header = '<table class="xwtable">'
    v_tab_thead = '<thead>{0}</thead>'
    v_tab_tbody = '<tbody>'
    v_sql = '''SELECT 
                  CONCAT(t.market_id,'') AS '项目编码',
                  ''                     AS '项目名称',
                  t.db                   AS '数据库名',
                  'dbo_summary_day'      AS '表名',
                  t.countdate            AS '最近操作时间',
                  t.amount               AS 'T-2天同步记录',
                  t.`state`              AS '同步状态'
               FROM sync.t_sync_flow_stats t
               WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_flow_stats)
                 AND t.db IN('hopsonone_flow_bj','hopsonone_flow_sh','hopsonone_flow_cd','hopsonone_flow_gz','hopsonone_flow_zc')
            '''
    cr.execute(v_sql)
    rs = cr.fetchall()
    desc = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'

    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[6] == '×':
            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                                 <td><font color="red">{0}</font></td>
                                 <td><font color="red">{1}</font></td>
                                 <td><font color="red">{2}</font></td>
                                 <td><font color="red">{3}</font></td>
                                 <td><font color="red">{4}</font></td>
                                 <td><font color="red">{5}</font></td>
                                 <td><font color="red">{6}</font></td>
                           </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                               <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                               <td>{4}</td><td>{5}</td><td>{6}</td>
                             </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
    v_flow = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$FLOW$$', v_flow)

    # 实时客流
    v_tab_header = '<table class="xwtable">'
    v_tab_thead = '<thead>{0}</thead>'
    v_tab_tbody = '<tbody>'
    v_sql = '''SELECT 
                      CONCAT(t.market_id,'') AS '项目编码',
                      ''                     AS '项目名称',
                      t.db                   AS '数据库名',
                      'dbo_summary_thirty'   AS '表名',
                      t.countdate            AS '最近操作时间',
                      t.amount               AS '最近1小时同步记录',
                      t.`state`              AS '同步状态'
                   FROM sync.t_sync_flow_stats t
                   WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_flow_stats)
                     AND t.db IN('hopsonone_flow_bj_real_time','hopsonone_flow_sh_real_time',
                                 'hopsonone_flow_cd_real_time','hopsonone_flow_gz_real_time','hopsonone_flow_zc_real_time')
                '''
    cr.execute(v_sql)
    rs = cr.fetchall()
    desc = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'

    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[6] == '×':
            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                                 <td><font color="red">{0}</font></td>
                                 <td><font color="red">{1}</font></td>
                                 <td><font color="red">{2}</font></td>
                                 <td><font color="red">{3}</font></td>
                                 <td><font color="red">{4}</font></td>
                                 <td><font color="red">{5}</font></td>
                                 <td><font color="red">{6}</font></td>
                           </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                               <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                               <td>{4}</td><td>{5}</td><td>{6}</td>
                             </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
    v_flow = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$REALF$$', v_flow)

    # 销售数据
    v_tab_header = '<table class="xwtable">'
    v_tab_thead = '<thead>{0}</thead>'
    v_tab_tbody = '<tbody>'
    v_sql = '''SELECT 
                         CONCAT(t.market_id,'') AS '项目编码',
                         ''                     AS '项目名称',
                         t.db                   AS '数据库名',
                         t.tname                AS '表名',
                         t.update_time          AS '最近操作时间',
                         t.amount_62_day        AS '最近62天同步记录',
                         t.state                AS '同步状态'
                      FROM sync.t_sync_sales_stats t
                      WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_sales_stats)
                   '''
    cr.execute(v_sql)
    rs = cr.fetchall()
    desc = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'

    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[6] == '×':
            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                                 <td><font color="red">{0}</font></td>
                                 <td><font color="red">{1}</font></td>
                                 <td><font color="red">{2}</font></td>
                                 <td><font color="red">{3}</font></td>
                                 <td><font color="red">{4}</font></td>
                                 <td><font color="red">{5}</font></td>
                                 <td><font color="red">{6}</font></td>
                           </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                               <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                               <td>{4}</td><td>{5}</td><td>{6}</td>
                             </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
    v_flow = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$SALES$$', v_flow)

    #会员数据
    v_tab_header = '<table class="xwtable">'
    v_tab_thead = '<thead>{0}</thead>'
    v_tab_tbody = '<tbody>'
    v_sql = '''SELECT 
                            CONCAT(t.market_id,'') AS '项目编码',
                            ''                     AS '项目名称',
                            t.db                   AS '数据库名',
                            t.tname                AS '表名',
                            t.update_time          AS '最近操作时间',
                            t.amount               AS '最近30天同步记录',
                            t.state                AS '同步状态'
                         FROM sync.t_sync_member_stats t
                         WHERE create_date=(SELECT MAX(create_date) FROM sync.t_sync_member_stats)
                      '''
    cr.execute(v_sql)
    rs = cr.fetchall()
    desc = cr.description
    v_row = '<tr>'
    for k in range(len(desc)):
        if k == 0:
            v_row = v_row + '<th width=5%>' + str(desc[k][0]) + '</th>'
        elif k == 1:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'
        elif k == 2:
            v_row = v_row + '<th width=15%>' + str(desc[k][0]) + '</th>'
        else:
            v_row = v_row + '<th width=10%>' + str(desc[k][0]) + '</th>'

    v_row = v_row + '</tr>'
    v_tab_thead = v_tab_thead.format(v_row)

    for i in list(rs):
        if i[6] == '×':
            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                                 <td><font color="red">{0}</font></td>
                                 <td><font color="red">{1}</font></td>
                                 <td><font color="red">{2}</font></td>
                                 <td><font color="red">{3}</font></td>
                                 <td><font color="red">{4}</font></td>
                                 <td><font color="red">{5}</font></td>
                                 <td><font color="red">{6}</font></td>
                           </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
        else:

            v_tab_tbody = v_tab_tbody + \
                          '''<tr>
                               <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>
                               <td>{4}</td><td>{5}</td><td>{6}</td>
                             </tr>  
                          '''.format(i[0], get_market_name(i[0]), i[2], i[3], i[4], i[5], i[6])
            v_tab_tbody = v_tab_tbody + '</tbody>'
    v_flow = v_tab_header + v_tab_thead + v_tab_tbody + '</table>'
    v_html = v_html.replace('$$MEMBERS$$', v_flow)
    cr.close()
    cr_biz.close()
    print(v_html)
    return v_html

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    config['db_bi']         = get_db(cfg.get("sync","db_bi"))
    config['db_biz']        = get_db(cfg.get("sync", "db_biz"))
    config['market_id']     = cfg.get("sync", "market_id")
    config['market_id_tjd'] = cfg.get("sync", "market_id_tjd")
    config['market_id_biz'] = cfg.get("sync", "market_id_biz")
    config['market_id_flow'] = cfg.get("sync", "market_id_flow")
    config['market_id_real_flow'] = cfg.get("sync", "market_id_real_flow")
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
    cr.execute("delete from sync.t_sync_real_park_stats where create_date='{0}'".format(get_max_sync_rq(config)))
    for p in config['market_id'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_status    = get_sync_status(config,v_market_id,v_market_db)
        v_stats_sql ='''insert into sync.t_sync_real_park_stats
                            (market_id,db,intime,outtime,chargetime,optdate,amount_today,amount_half_hour,state,create_date)
                        SELECT
                           MAX(market_id)  AS market_id,
                           '{0}'           as dbname,
                           MAX(intime)     AS intime,
                           MAX(outtime)    AS outtime,
                           MAX(chargetime) AS chargetime ,
                           MAX(optdate)    AS optdate,
                           (SELECT COUNT(0)
                             FROM {1}.tc_record_cleaning 
                            WHERE market_id=t.market_id AND optdate>DATE(NOW())) AS amount_today,
                           (SELECT COUNT(0)
                              FROM {2}.tc_record_cleaning 
                            WHERE market_id=t.market_id AND optdate>DATE_ADD(NOW(),INTERVAL -30 MINUTE)) AS amount_half_hour,
                            '{3}',        
                           CONCAT(DATE_FORMAT(NOW(),'%H'),':0:0') AS create_time
                         FROM {4}.tc_record_cleaning t
                        WHERE t.market_id='{5}'
                     '''.format(v_market_db,v_market_db,v_market_db,v_status,v_market_db,v_market_id)
        #print(v_stats_sql)
        cr.execute(v_stats_sql)
        print('{0} write_stats insert complete'.format(v_market_db))
    db.commit()

def write_flow_stats(config):
    db  = config['db_bi']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_flow_stats where create_date='{0}'".format(get_max_flow_rq(config)))
    for p in config['market_id_flow'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_status    = get_flow_status(config,v_market_id,v_market_db)
        v_create    = get_create_time()
        v_stats_sql ='''insert into sync.t_sync_flow_stats
                            (market_id,db,countdate,amount,state,create_date)
                        SELECT
                           MAX(market_id)  as market_id,
                           '{0}'           as dbname,
                           MAX(countdate)  as countdate,
                           (SELECT COUNT(0)
                             FROM {1}.dbo_summary_day 
                            WHERE market_id=t.market_id AND countdate>DATE(DATE_ADD(NOW(),INTERVAL -2 DAY))) AS amount_today_2,
                            '{2}',        
                            '{3}' AS create_time
                         FROM {4}.dbo_summary_day t
                        WHERE t.market_id='{5}'
                     '''.format(v_market_db,v_market_db,v_status,v_create,v_market_db,v_market_id)
        cr.execute(v_stats_sql)
        print('{0} write_flow_stats insert complete'.format(v_market_db))
    db.commit()


def write_flow_real_stats(config):
    db  = config['db_bi']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_flow_stats where create_date='{0}'".format(get_max_real_flow_rq(config)))
    for p in config['market_id_real_flow'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_status    = get_real_flow_status(config,v_market_id,v_market_db)
        v_create    = get_create_time()
        v_stats_sql ='''insert into sync.t_sync_flow_stats
                            (market_id,db,countdate,amount,state,create_date)
                        SELECT
                           MAX(market_id)  as market_id,
                           '{0}'           as dbname,
                           MAX(countdate)  as countdate,
                           (SELECT COUNT(0)
                             FROM {1}.dbo_summary_thirty 
                            WHERE market_id=t.market_id AND countdate>DATE(DATE_ADD(NOW(),INTERVAL -1 hour))) AS amount_hour_1,
                            '{2}',        
                            '{3}' AS create_time
                         FROM {4}.dbo_summary_thirty t
                        WHERE t.market_id='{5}'
                     '''.format(v_market_db,v_market_db,v_status,v_create,v_market_db,v_market_id)
        cr.execute(v_stats_sql)
        print('{0} write_flow_real_stats insert complete'.format(v_market_db))
    db.commit()

def get_tjd_intime(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(datein) FROM {0}.dbo_park_carlog'.format(p_market_db)
    print(sql)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_outtime(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(dateout) FROM {0}.dbo_park_caroutlog'.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_spetime(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(createdate) FROM {0}.dbo_oper_datalog'.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_nooutpay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(paydate) FROM {0}.dbo_park_paylog'.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_outpay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(paydate) FROM {0}.dbo_park_payoutlog'.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def get_tjd_spepay_time(config,p_market_db):
    db = config['db_bi']
    cr = db.cursor()
    sql = 'SELECT MAX(createdate) FROM {0}.dbo_park_specicarlog'.format(p_market_db)
    cr.execute(sql)
    rs = cr.fetchone()
    db.commit()
    cr.close()
    return rs[0]

def write_stats_tjd(config):
    db  = config['db_bi']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_real_tjd_stats where create_date='{0}'".format(get_max_sync_tjd_rq(config)))
    for p in config['market_id_tjd'].split(","):
        print(p)
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_intime    = get_tjd_intime(config, v_market_db)
        v_outtime   = get_tjd_outtime(config, v_market_db)
        v_spetime   = get_tjd_spetime(config, v_market_db)
        v_nooutpay  = get_tjd_nooutpay_time(config, v_market_db)
        v_outpay    = get_tjd_outpay_time(config, v_market_db)
        v_spepay    = get_tjd_spepay_time(config, v_market_db)
        v_create    = get_create_time()
        v_stats_sql ='''insert into sync.t_sync_real_tjd_stats
                            (market_id,db,intime,outtime,spetime,nooutpay_time,outpay_time,spepay_time,create_date)
                        values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')
                     '''.format(v_market_id,v_market_db,v_intime,v_outtime,v_spetime,v_nooutpay,v_outpay,v_spepay,v_create)
        print(v_stats_sql)
        cr.execute(v_stats_sql)
        print('{0} write_stats_tjd insert complete'.format(v_market_db))
    db.commit()

def write_stats_biz(config):
    db  = config['db_biz']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_park_stats where create_date='{0}'".format(get_max_sync_tjd_rq(config)))
    for p in config['market_id_biz'].split(","):
        v_market_id = p.split(':')[0]
        v_market_db = p.split(':')[1]
        v_state     = get_sync_status_biz(config, v_market_id, v_market_db)
        v_create    = get_create_time()
        v_stats_sql ='''insert into sync.t_sync_park_stats
                          (market_id,db,optdate,amount_today,amount_half_hour,state,create_date)
                        select
                           MAX(market_id)  AS market_id,
                           '{0}',  
                           MAX(optdate)    AS optdate,
                           (SELECT COUNT(0)
                             FROM {1}.dbo_userwork 
                            WHERE market_id=t.market_id AND optdate>DATE(NOW())) AS amount_today,
                           (SELECT COUNT(0)
                              FROM {2}.dbo_userwork 
                            WHERE market_id=t.market_id AND optdate>DATE_ADD(NOW(),INTERVAL -30 MINUTE)) AS amount_half_hour,
                           '{3}',        
                           '{4}' AS create_time
                         from {5}.dbo_userwork t
                        where t.market_id='{6}'
                     '''.format(v_market_db,v_market_db,v_market_db,v_state,v_create,v_market_db,v_market_id)
        print(v_stats_sql)
        cr.execute(v_stats_sql)
        print('{0} write_stats_biz insert complete'.format(v_market_db))
    db.commit()


def get_rq_from_biz(config,db,tab,v_stat_sql):
    db_biz = config['db_biz']
    db_bi  = config['db_bi']
    cr_biz = db_biz.cursor()
    cr_biz.execute(v_stat_sql)
    rs_biz = cr_biz.fetchone()
    if db=='shop_side_operation':
        v_ins_sql="""
                    insert into sync.t_sync_biz_to_bi_stats
                        (market_id,db,tname,trade_date,amount_today,amount_one_hour,state,create_date)
                    values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','','',)
                  """.format('000',db,tab,rs_biz[0],rs_biz[1],rs_biz[2],rs_biz[3],rs_biz[4])


def write_sales_stats(config):
    db  = config['db_bi']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_sales_stats where create_date='{0}'".format(get_max_sales_rq(config)))
    v_sql  = """INSERT INTO sync.t_sync_sales_stats
                   (market_id,db,tname,update_time,amount_62_day,state,create_date)
                SELECT 
                    market_id,
                    'shop_side_operation' AS 'db',
                    'sales_report_day'    AS 'tname',
                    MAX(update_time),
                    (SELECT COUNT(0)
                     FROM shop_side_operation.sales_report_day     
                    WHERE  trade_date>DATE_ADD(NOW(),INTERVAL -62 DAY) AND market_id=t.market_id ) AS amount_62_day,
                    CASE WHEN MAX(update_time)<DATE(NOW()) THEN
                      '×'
                    ELSE
                      '√'
                    END STATUS,    
                     NOW() AS 'optdate'
                FROM shop_side_operation.`sales_report_day`  t 
                GROUP BY market_id
            """
    cr.execute(v_sql)
    db.commit()
    cr.close()
    print('write_sales_stauts insert complete')

def write_members_stats(config):
    db  = config['db_bi']
    cr  = db.cursor()
    cr.execute("delete from sync.t_sync_member_stats where create_date='{0}'".format(get_max_member_rq(config)))
    v_sql  = """INSERT INTO sync.t_sync_member_stats
                   (market_id,db,tname,update_time,amount,state,create_date)
                SELECT 
                    market_id,
                    'hopsonone_members' AS 'db',
                    'members'    AS 'tname',
                    MAX(create_time),
                    (SELECT COUNT(0)
                     FROM hopsonone_members.members     
                    WHERE  create_time>DATE_ADD(NOW(),INTERVAL -30 DAY) AND market_id=t.market_id ) AS amount_30_day,
                    CASE WHEN MAX(create_time)<DATE_ADD(NOW(),INTERVAL -30 DAY)THEN
                      '×'
                    ELSE
                      '√'
                    END STATUS,    
                     NOW() AS 'optdate'
                FROM hopsonone_members.`members`  t 
                GROUP BY market_id
            """
    cr.execute(v_sql)
    db.commit()
    cr.close()
    print('write_members_stauts insert complete')

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

    #实时捷顺车流
    write_stats(config)

    #实时停简单车流
    write_stats_tjd(config)

    #业务同步
    write_stats_biz(config)

    #离线客流
    write_flow_stats(config)

    #实时客流
    write_flow_real_stats(config)

    #销售同步[biz->bi]
    write_sales_stats(config)

    #会员同步[biz->bi]
    write_members_stats(config)

    #邮件提醒
    send_mail465(config['send_user'], config['send_pass'],
                   config['acpt_user'], config['mail_title'],
                   get_html_contents(config))



if __name__ == "__main__":
     main()
