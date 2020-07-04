#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Func : 数据同步检测程序
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

def set_sync_flag(config,flag):
    cr = config['db_mysql_desc3'].cursor()
    sql="""select count(0) from sync.t_mysql2mysql_sync_log 
            where market_id={0} and type='{1}'""".format(config['sync_col_val'],config['sync_type'])
    cr.execute(sql)
    rs=cr.fetchone()
    if rs[0]>0:
        cr.execute("""update sync.t_mysql2mysql_sync_log 
                           set status='{0}',last_update_date=now() 
                         where market_id={1} and type='{2}'
                   """.format(flag,config['sync_col_val'], config['sync_type']))

    else:
        cr.execute("""insert into sync.t_mysql2mysql_sync_log(market_id,type,status,create_date,last_update_date) 
                        values({0},'{1}','{2}',now(),now())""".format(config['sync_col_val'],config['sync_type'],flag))
    config['db_mysql_desc3'].commit()
    cr.close()

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

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_db(dbstr):
    db_ip      = dbstr.split(':')[0]
    db_port    = dbstr.split(':')[1]
    db_service = dbstr.split(':')[2]
    db_user    = dbstr.split(':')[3]
    db_pass    = dbstr.split(':')[4]
    conn = pymysql.connect(host=db_ip, port=int(db_port), user=db_user, passwd=db_pass, db=db_service, charset='utf8')
    return conn

def write_log(config,msg,show=True):
    if show:
       print(msg)
    cr = config['db_mysql_desc'].cursor()
    cr.execute("insert into sync.t_sync_log(message,type,create_time) values('{0}','{1}',now())".format(msg,config['sync_type']))
    cr.close()

def get_sync_log(config):
    cr   = config['db_mysql_desc'].cursor()
    sql  = """select CONCAT(create_time,' => ',message) AS msg
              from  sync.t_sync_log
             where create_time>=DATE_SUB(NOW(), INTERVAL 30 MINUTE) 
               and type='{0}'
             order by id""".format(config['sync_type'])
    v_log = ''
    cr.execute(sql)
    rs=cr.fetchall()
    for i in list(rs):
        v_log=v_log+i[0]+'<br>'
    cr.close()
    return v_log[0:-4]

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
                       <h1 align="center">经营日报数据同步检测情况表</h1>
                       <p></p>
                       $$TABLE$$
                       <h3>说明：</h3>
                       <ul>
                           <li> 1.各项目客流、车流数一致则不会触发调用大数据脚本</li> 
                           <li> 2.任一项目客流、车流数不一致会触发大数据脚本重新计算</li>
                           <li> 3.各项目客流、车流数为0会触发大数据脚本重新计算</li> 
                           <li> 4.脚本每天凌晨4点定时调用</li> 
                        </ul>
                        <p>
                        $$SCRIPT$$  
                    </body>
                  </html>
               '''
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '<thead>{0}</thead>'
    v_tab_tbody   = '<tbody>'
    v_sql  = '''SELECT 
                  market_id as '项目编码',
                  CASE WHEN t.market_id='218' THEN '北京朝阳合生汇'
                       WHEN t.market_id='110' THEN '上海五角场'
                       WHEN t.market_id='108' THEN '成都珠江广场'
                       WHEN t.market_id='132' THEN '广州嘉和南'
                       WHEN t.market_id='234' THEN '上海青浦米格'
                       WHEN t.market_id='164' THEN '北京合生广场'
                       WHEN t.market_id='213' THEN '合生新天地'
                       WHEN t.market_id='237' THEN '广州增城合生汇'
                  END AS '项目名称',
                  t.tjrq             as '统计日期',
                  t.bi_flow          as '项目客流数',
                  t.bi_park          as '项目车流数', 
                  t.bi_sales_amount  as '项目销售额',
                  t.bi_sales_count   as '项目销售笔数',
                  t.big_flow         as '大数据客流' ,
                  t.big_park         as '大数据车流',
                  t.big_sales_amount as '大数据销售额',
                  t.big_sales_count  as '大数据销售笔数',        
                  CASE WHEN t.bi_flow=t.big_flow  THEN '√' ELSE  '×' END AS '客流比对',
                  CASE WHEN t.bi_park=t.big_park  THEN '√' ELSE  '×' END AS '车流对比',
                  CASE WHEN t.bi_sales_amount=t.big_sales_amount  THEN '√' ELSE  '×' END AS '销售额对比',
                  CASE WHEN t.bi_sales_count=t.big_sales_count  THEN '√' ELSE  '×'   END AS '销售笔数比对'
                FROM  sync.t_sync_stats t
                WHERE tjrq >= CONCAT(DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                  AND tjrq <= CONCAT(DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
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
        v_tab_tbody = v_tab_tbody +\
                '''<tr>
                     <td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td>
                     <td>{5}</td><td>{6}</td><td>{7}</td><td>{8}</td><td>{9}</td>
                     <td>{10}</td><td>{11}</td><td>{12}</td><td>{13}</td><td>{14}</td>
                   </tr>  
                '''.format(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10], i[11], i[12], i[13], i[14])
        v_tab_tbody=v_tab_tbody+'</tbody>'

    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html=v_html.replace('$$TABLE$$',v_table)

    if check_valid(config)>0:
       print('Loading hopson_market_analysis.sh script....')
       os.system(config['script_file'])
       v_note ='''<h3>备注:</h3>
                  <ul>
                     <li> <span style="color:#F00" >1.同步异常：已触发再次调用大数据分析脚本!</span>
                     <li> <span style="color:#F00" >2.调用时间：{0}</span>
                     <li> <span style="color:#F00" >3.调用脚本：{1}</span>
                  </ul>  
               '''.format(get_time(),config['script_file'])
       v_html = v_html.replace('$$SCRIPT$$', v_note)
    else:
       v_html = v_html.replace('$$SCRIPT$$', '')

    cr.close()
    return v_html

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    config['db_bi']       = get_db(cfg.get("sync","db_bi"))
    config['market_id']   = cfg.get("sync", "market_id")
    config['send_user']   = cfg.get("sync", "send_mail_user")
    config['send_pass']   = cfg.get("sync", "send_mail_pass")
    config['acpt_user']   = cfg.get("sync", "acpt_mail_user")
    config['mail_title']  = cfg.get("sync", "mail_title")
    config['script_file'] = cfg.get("sync", "script_file")
    config['status_file'] = cfg.get("sync", "status_file")
    return config

def get_mysql_tab_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_sync_table_total_rows(db,tab,v_where):
    cr_source = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

#返回BI T-2日客流统计
def get_bi_flow_stats(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    v_stats = ''
    if p_market_id == '218':
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_bj.dbo_summary_day
                         where market_id={0} and sitetype=300
                           and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                           and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                 '''.format(p_market_id)
    elif p_market_id == '110':
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_sh.dbo_summary_day
                         where market_id={0} and sitetype=300
                           and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                           and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                 '''.format(p_market_id)
    elif p_market_id == '108':
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_cd.dbo_summary_day
                         where market_id={0} and sitetype=300
                           and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                           and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                 '''.format(p_market_id)
    elif p_market_id == '132':
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_gz.dbo_summary_day
                         where market_id={0} and sitetype=300
                           and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                           and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                 '''.format(p_market_id)
    elif p_market_id in('234','164','213'):
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_tmp.dbo_summary_day
                        where market_id={0} and sitetype=300
                          and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                          and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                '''.format(p_market_id)
    elif p_market_id == '237':
       v_stats = '''select IFNULL(SUM(insum),0) from hopsonone_flow_zc.dbo_summary_day
                         where market_id={0} and sitetype=300
                           and countdate>=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                           and countdate<=concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                 '''.format(p_market_id)
       print(v_stats)

    try:

        cr_desc.execute(v_stats)
        rs=cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2日车流入口统计
def get_bi_park_stats(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    v_stats = ''
    if p_market_id=='218':
        v_stats = '''select COUNT( platenumpic) 
                       from hopsonone_park_bj_tjd.dbo_park_caroutrun
                      where controlid IN (2, 14, 5, 9) 
                        and platenumpic != ''
                        and passdate >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                        and passdate <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                  '''
    elif p_market_id=='110':
        v_stats = '''select count(0) 
                       from hopsonone_park_sh.tc_record_cleaning
                      where market_id={0} 
                        and indeviceentrytype=1
                        and intime >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                        and intime <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                  '''.format(p_market_id)
    elif p_market_id=='108':
        v_stats = '''select count(0) 
                       from hopsonone_park_cd.tc_record_cleaning
                      where market_id={0} 
                        and indeviceentrytype=1
                        and intime >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                        and intime <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                  '''.format(p_market_id)
    elif p_market_id=='132':
        v_stats = '''select count(0) 
                       from hopsonone_park_gz.tc_record_cleaning
                      where market_id={0} 
                        and indeviceentrytype=1
                        and intime >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                        and intime <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                  '''.format(p_market_id)
    elif p_market_id in('164'):
        v_stats = '''select park_flow
                       from hopsonone_park_tmp.traffic
                      where market_id={0} 
                        and date(park_date) >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                        and date(park_date) <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                        limit 1
                  '''.format(p_market_id)
    elif p_market_id in ('213'):
        v_stats = '''select count(0) 
                          from hopsonone_park_bj_wangjing.tc_record_cleaning
                         where market_id={0} 
                            and indeviceentrytype=1
                            and intime >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                            and intime <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                     '''.format(p_market_id)
    elif p_market_id in('234'):
        v_stats = '''select COUNT( platenumpic) 
                      from hopsonone_park_sh_tjd.dbo_park_caroutrun
                     where controlid IN (2, 3, 4, 7) 
                       and platenumpic != ''
                       and passdate >= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                       and passdate <= concat(date_format(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                    '''

    try:
        cr_desc.execute(v_stats)
        rs=cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2销售额日统计
def get_bi_sales_amount_stats(config,p_market_id):
    d_tjrq = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    v_stats = '''SELECT IFNULL(SUM(b.adjust_tax_sales_amount_month),0) 
                   FROM shop_side_operation.sales_report_day a,
                        shop_side_operation.sales_report_details b
                 WHERE a.trade_no=b.trade_no
                   AND a.trade_date='{0}'
                   AND a.audit_status=2
                   AND a.type=0
                   AND a.market_id={1}
                  '''.format(d_tjrq, p_market_id)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2销售笔数日统计
def get_bi_sales_count_stats(config,p_market_id):
    d_tjrq  = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    v_stats = '''SELECT IFNULL(SUM(b.adjust_trade_count_month),0)
                 FROM shop_side_operation.sales_report_day a,
                      shop_side_operation.sales_report_details b
                    WHERE a.trade_no=b.trade_no
                      AND a.trade_date='{0}'
                      AND a.audit_status=2
                      AND a.type=0
                      AND a.market_id={1}
                  '''.format(d_tjrq,p_market_id)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2日大数据库统计客流入口统计
def get_bi_bigdata_flow(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    d_tjrq  = get_tjrq()
    v_stats = '''SELECT passenger_flow_count 
                   FROM hopsonone_market_analysis.market_analysis_day 
                 WHERE market_id={0} AND DATE='{1}'
              '''.format(p_market_id,d_tjrq)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2日大数据库统计车流入口统计
def get_bi_bigdata_park(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    d_tjrq  = get_tjrq()
    v_stats = '''SELECT freight_flow_count 
                    FROM hopsonone_market_analysis.market_analysis_day 
                      WHERE market_id={0} AND DATE='{1}'
              '''.format(p_market_id, d_tjrq)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2日大数据库统计销售额统计
def get_bi_bigdata_sales_amount(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    d_tjrq  = get_tjrq()
    v_stats = '''SELECT total_sales_amount 
                        FROM hopsonone_market_analysis.market_analysis_day 
                          WHERE market_id={0} AND DATE='{1}'
                  '''.format(p_market_id, d_tjrq)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

#返回BI T-2日大数据库统计车流入口统计
def get_bi_bigdata_sales_count(config,p_market_id):
    db_desc = config['db_bi']
    cr_desc = db_desc.cursor()
    d_tjrq = get_tjrq()
    v_stats = '''SELECT total_trade_count 
                            FROM hopsonone_market_analysis.market_analysis_day 
                              WHERE market_id={0} AND DATE='{1}'
                      '''.format(p_market_id, d_tjrq)
    try:
        cr_desc.execute(v_stats)
        rs = cr_desc.fetchone()
        cr_desc.close()
        return rs[0]
    except:
        return 0

def get_tjrq():
    return (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')

##############################################################################
#   1、写项目MySQL客流T-2统计数据至生产BI库t_sync_stats表中
#   2、写项目MySQL车流T-2统计数据至生产BI库t_sync_stats表中
#   3、写大数据分析后客流、车流、销售额、销售笔数至生产BI库t_sync_stats表中
#   4、临晨3点调用.脚本部署在阿里云内网服务器上
###############################################################################
def write_stats(config):
    db     = config['db_bi']
    cr     = db.cursor()
    for p_market_id in config['market_id'].split(","):
        d_tjrq                = get_tjrq()
        n_bi_flow_stats       = get_bi_flow_stats(config,p_market_id)
        n_bi_park_stats       = get_bi_park_stats(config,p_market_id)
        n_bi_sales_amount     = get_bi_sales_amount_stats(config,p_market_id)
        n_bi_sales_count      = get_bi_sales_count_stats(config,p_market_id)
        n_bi_big_flow         = get_bi_bigdata_flow(config,p_market_id)
        n_bi_big_park         = get_bi_bigdata_park(config,p_market_id)
        n_bi_big_sales_amount = get_bi_bigdata_sales_amount(config,p_market_id)
        n_bi_big_sales_count  = get_bi_bigdata_sales_count(config,p_market_id)

        v_stats_sql ='''select count(0) from sync.t_sync_stats where market_id={0} and tjrq='{1}'
                     '''.format(p_market_id,d_tjrq)

        v_stats_ins ='''insert into sync.t_sync_stats(
                                    market_id,tjrq,bi_flow,bi_park,bi_sales_amount,bi_sales_count,
                                    big_flow,big_park,big_sales_amount,big_sales_count,
                                    created_date,last_update_date)
                          values('{0}','{1}',{2},{3},{4},{5},
                                  {6},{7},{8},{9}, 
                                  now(),now())
                     '''.format(p_market_id, d_tjrq,
                                n_bi_flow_stats,n_bi_park_stats,n_bi_sales_amount,n_bi_sales_count,
                                n_bi_big_flow,n_bi_big_park, n_bi_big_sales_amount,n_bi_big_sales_count
                                )

        v_stats_upd ='''update sync.t_sync_stats
                           set bi_flow={0},
                               bi_park={1},
                               bi_sales_amount={2},
                               bi_sales_count={3},
                               big_flow={4},
                               big_park={5},
                               big_sales_amount={6},
                               big_sales_count={7},                       
                               last_update_date=now()
                         where market_id={8} and tjrq='{9}'
                     '''.format(n_bi_flow_stats,
                                n_bi_park_stats,
                                n_bi_sales_amount,
                                n_bi_sales_count,
                                n_bi_big_flow,
                                n_bi_big_park,
                                n_bi_big_sales_amount,
                                n_bi_big_sales_count,
                                p_market_id,d_tjrq)

        cr.execute(v_stats_sql)
        rs=cr.fetchone()
        if rs[0]>0:
           print(v_stats_upd)
           cr.execute(v_stats_upd)
           print('{0} write_stats update complete'.format(p_market_id))
        else:
           print(v_stats_ins)
           cr.execute(v_stats_ins)
           print('{0} write_stats insert complete'.format(p_market_id))
        db.commit()

def check_valid(config):
    db = config['db_bi']
    cr = db.cursor()
    sql='''SELECT COUNT(0)
            FROM (SELECT 
                      market_id,
                      CASE WHEN t.bi_flow=t.big_flow and t.bi_flow>0 THEN '√' ELSE  '×' END AS 'flow',
                      CASE WHEN t.bi_park=t.big_park and t.bi_park>0 THEN '√' ELSE  '×' END AS 'park'
                    FROM  sync.t_sync_stats t
                    WHERE tjrq >= CONCAT(DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 0:0:0')
                      AND tjrq <= CONCAT(DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 2 DAY),'%Y-%m-%d'),' 23:59:59')
                  ) X WHERE x.flow='×' OR x.park='×'
         '''
    cr.execute(sql)
    rs=cr.fetchone()
    db.commit()
    db.close()
    return rs[0]


#####################################################################################################
#  1、更新BI库t_sync_stats表中T-2销售数据、销售笔。
#  2、BI库客流、车流、销售与大数据统计结果表对比（查询表：t_sync_stats表实现）
#  3、将以上比对结果写入BI库中t_sync_stats_log表中
#  4、根据BI库中t_sync_stats_log表中的数据生成邮件正文，如果统计有问题，通过API调用大数据统计任务
#     并获取统计结果，统计时长，最后将结果写入邮件正文中。
####################################################################################################
def check_sync(config):
    write_stats(config)

def init(config):
    config = get_config(config)
    print_dict(config)
    return config

def check_hadoop_status(config):
    r=os.system('ls -l {0} &>/dev/null'.format(config['status_file']))
    if r==0:
       return True
    else:
       return False

def main():
    #init variable
    config = ""
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]

    #初始化
    config=init(config)
    
    if not check_hadoop_status(config):
      #sync check
      check_sync(config)

      #send mail
      send_mail465(config['send_user'], config['send_pass'],
                   config['acpt_user'], config['mail_title'],
                   get_html_contents(config))
    else:
      print('Hadoop task running!')


if __name__ == "__main__":
     main()
