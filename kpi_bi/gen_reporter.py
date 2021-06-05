#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/29 9:28
# @Author : ma.fei
# @File : gen_reporter.py
# @Software: PyCharm

'''
  功能：按不同项目生成各各指标，定时发送邮件
    SELECT * FROM `kpi_po`  ORDER BY market_id+0;
    SELECT * FROM `kpi_item` ORDER BY CODE;
    SELECT * FROM kpi_item_sql WHERE if_stat='Y' ORDER BY item_code
    SELECT * FROM `kpi_po_mx`
    SELECT a.* FROM kpi_po_mx a,kpi_po b WHERE a.market_id=b.market_id ORDER BY b.sxh,item_code+0
    SELECT a.* FROM kpi_po_hz a,kpi_po b WHERE a.market_id=b.market_id ORDER BY b.sxh,item_code+0

'''

import json
import pymysql
import datetime
import calendar
import smtplib
from email.mime.text import MIMEText

# 报表月
g_month  = 5

dblog = {
    "db_ip"        : "rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com",
    "db_port"      : "3306",
    "db_user"      : "hopsononebi_2019",
    "db_pass"      : "xMEAnLk8SOfa8bEwq2Xl6LpuI8QWXb0",
    "db_service"   : "hopsonone_kpi",
    "db_charset"   : "utf8"
}

# 获取第一天和最后一天
def getFirstAndLastDay(p_month):
    # 获取当前年份
    year = datetime.date.today().year
    # 获取当前月的第一天的星期和当月总天数
    weekDay,monthCountDay = calendar.monthrange(year,p_month)
    # 获取当前月份第一天
    firstDay = datetime.date(year,p_month,day=1).strftime("%Y-%m-%d")
    # 获取当前月份最后一天
    lastDay = datetime.date(year,p_month,day=monthCountDay).strftime("%Y-%m-%d")
    # 返回第一天和最后一天
    return firstDay,lastDay

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
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

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_templete():
    return '''<html>
                    <head>
                       <style type="text/css">
                           .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                           .xwtable thead td {font-size: 12px;color: #333333;
                                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable thead th {font-size: 12px;color: #333333;
                                              text-align: center;background-color: #C0C0C0;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                           .xwtable tbody tr.alt-row {background: #f2f7fc;}
                           .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                       </style>
                    </head>
                    <body>
                       <h3 align="center"><b>商管BI-KPI统计情况表</b></h3>
                       <p></p>
                       <br>统计日期：<br>
                           &nbsp;&nbsp;<span>$$TJRQ$$</span>
                       
                       <br>统计范围：<br>
                           &nbsp;&nbsp;本月：<span>$$TJRQQ$$～$$TJRQZ$$</span><br>
                           &nbsp;&nbsp;累计：<span>$$LJTJRQQ$$～$$TJRQZ$$</span>
                       <p>
                       $$TABLE$$
                    </body>
                  </html>
               '''

def get_hz_log(dbd):
   cr=dbd.cursor()
   st="""select a.bbrq,a.month,
                a.market_id,a.market_name,
                a.item_code,a.item_name,
                a.goal,a.actual_completion,a.completion_rate,
                a.`annual target`,a.completion_sum_finish,a.completion_sum_rate,
                date_format(a.create_time,'%Y-%m-%d %H:%i:%s') as create_time,
                date_format(a.update_time,'%Y-%m-%d %H:%i:%s') as update_time
        from kpi_po_hz a,kpi_po b 
        WHERE a.market_id=b.market_id and a.month='{}' 
            and item_code not in('9','13','2.1','2.2','12.1','12.2') 
        ORDER BY b.sxh,a.item_code+0""".format(get_month(g_month))
   cr.execute(st)
   rs=cr.fetchall()
   return rs

def get_html_contents(p_dbd):
    v_html = get_templete()
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '''<thead>
                           <tr>
                               <th>报表日期</th>
                               <th>报表月</th>
                               <th>项目编码</th>
                               <th>项目名称</th>
                               <th>指标编码</th>
                               <th>指标名称</th>
                               <th>月度目标</th>
                               <th>月度完成</th>
                               <th>月度完成率</th>
                               <th>年度指标</th>
                               <th>年度完成</th>
                               <th>年度完成率</th>
                               <th>生成时间</th>
                               <th>最后更新时间</th>
                           </tr>  
                    </thead>'''
    v_tab_tbody   = '<tbody>'
    for log in list(get_hz_log(p_dbd)):
        v_tab_tbody = v_tab_tbody + \
            '''<tr>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
             </tr>'''.format(
                             log.get('bbrq'),
                             log.get('month'),
                             log.get('market_id'),
                             log.get('market_name'),
                             log.get('item_code'),
                             log.get('item_name'),
                             log.get('goal'),
                             log.get('actual_completion'),
                             log.get('completion_sum_rate'),
                             log.get('annual target'),
                             log.get('completion_sum_finish'),
                             log.get('completion_sum_rate'),
                             log.get('create_time'),
                             log.get('update_time'))

    v_tab_tbody = v_tab_tbody + '</tbody>'
    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html = v_html.replace('$$TABLE$$',v_table)
    v_html = v_html.replace('$$TJRQ$$',get_time())
    v_html = v_html.replace('$$TJRQQ$$', get_first_day(g_month))
    v_html = v_html.replace('$$TJRQZ$$', get_last_day(g_month))
    v_html = v_html.replace('$$LJTJRQQ$$', get_first_day_year())

    return v_html

def get_month(p_month):
    return get_first_day(p_month)[0:7]

def get_first_day_year():
    now = datetime.date.today()
    return datetime.datetime(now.year, 4, 1).strftime("%Y-%m-%d")

def get_first_day(p_month):
    return getFirstAndLastDay(p_month)[0]

def get_last_day(p_month):
    return getFirstAndLastDay(p_month)[1]+' 23:59:59'

def get_bbrq():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_db():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'])
    return conn

def get_db_dict():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'],
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_db():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'])
    return conn

def get_db_from_ds(p_ds):
    conn = pymysql.connect(host    = p_ds['ip'],
                           port    = int(p_ds['port']),
                           user    = p_ds['user'],
                           passwd  = p_ds['password'],
                           db      = p_ds['service'],
                           charset ='utf8')
    return conn

def aes_decrypt(p_db,p_password, p_key):
    st = """select aes_decrypt(unhex('{0}'),'{1}') as pass""".format(p_password, p_key[::-1])
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return str(rs['pass'], encoding="utf-8")

def get_ds_by_dsid(p_db,p_dsid):
    st ="""select cast(id as char) as dsid,
                  db_type,
                  db_desc,
                  ip,
                  port,
                  service,
                  user,
                  password,
                  status,
                  date_format(creation_date,'%Y-%m-%d %H:%i:%s') as creation_date,
                  creator,
                  date_format(last_update_date,'%Y-%m-%d %H:%i:%s') as last_update_date,
                  updator ,
                  db_env,
                  inst_type,
                  market_id,
                  proxy_status,
                  proxy_server
           from kpi_ds where id={0}""".format(p_dsid)
    cr=p_db.cursor()
    cr.execute(st)
    rs=cr.fetchone()
    rs['password'] =  aes_decrypt(p_db,rs['password'],rs['user'])
    rs['url'] = 'MySQL://{0}:{1}/{2}'.format(rs['ip'], rs['port'], rs['service'])
    cr.close()
    return rs

def get_bbtj_sql(p_db):
    st ="""select * from kpi_item_sql where item_type='1'  order by item_code"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_bbtj_sql_hst(p_db):
    st ="""select * from kpi_item_sql where item_type='2' order by item_code"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_value(p_dbd,p_dsid,p_st,p_st_sum):
    try:
        if p_st is None or p_st_sum is None :
           return '',''

        ds = get_ds_by_dsid(p_dbd,p_dsid)
        db = get_db_from_ds(ds)
        cr = db.cursor()
        cr.execute(p_st)
        rs1 = cr.fetchone()
        cr.execute(p_st_sum)
        rs2 = cr.fetchone()
        cr.close()
        val1 = 0 if rs1[0] is None else str(rs1[0])
        val2 = 0 if rs2[0] is None else str(rs2[0])
        return val1,val2
    except:
        return 0,0

def get_markets(dblog,flag):
    st = "select  * from `kpi_po` where if_stat='{}'  order by sxh".format(flag)
    cr = dblog.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def check_item_exists(db,po):
    cr = db.cursor()
    st = """select  count(0)  as rec from `kpi_po_mx` where market_id='{}' and item_code= '{}' and month='{}'
         """.format(po['market_id'],po['item_code'],get_month(g_month))
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    return rs['rec']

def check_hz_exists(db):
    cr = db.cursor()
    st = """select  count(0)  as rec from `kpi_po_hz` where `month`='{}'""".format(get_month(g_month))
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    return rs['rec']

def write_log(db,bb):
    cr = db.cursor()
    for po in bb:
        if check_item_exists(db,po)>0:
            print('update market:{} item:{} log , value={},sum={} ....'.format(po['market_id'],po['item_name'],po['value'],po['value_sum']))
            st = """update kpi_po_mx 
                       set  item_value='{}',
                            item_value_sum='{}',
                            market_name='{}',
                            update_time=now(),
                            bbrq='{}'
                      where market_id='{}' and item_code= '{}'  and month='{}'
                 """.format(po['value'],po['value_sum'],po['market_name'],get_bbrq(),po['market_id'], po['item_code'],get_month(g_month))
        else:
            print('insert market:{} item:{} log, value={},sum={}....'.format(po['market_id'],po['item_name'],po['value'],po['value_sum']))
            st = """insert into kpi_po_mx(market_id,market_name,bbrq,month,item_code,item_name,item_value,item_value_sum,create_time) 
                      values('{}','{}','{}','{}','{}','{}','{}','{}',now())
                   """.format(po['market_id'],po['market_name'],get_bbrq(),get_month(g_month),po['item_code'],po['item_name'],po['value'],po['value_sum'])
        cr.execute(st)
        db.commit()

def update_log(db):
    cr = db.cursor()
    st = """delete from kpi_po_mx where month='{}' and item_code in ('2','12','10','15')""".format(get_month(g_month))
    cr.execute(st)

    # calc project and region gmv
    st = """insert into kpi_po_mx(bbrq,month,market_id,market_name,item_code,item_name,item_value,item_value_sum,create_time,update_time)
            select '{}',
                    month,
                    market_id,
                    market_name,
                    '2' AS item_code,
                    '总GMV指标（万）' AS item_name,
                    round(SUM(item_value+0),2) AS item_value,
                    round(SUM(item_value_sum+0),2) AS item_value_sum,
                    NOW() AS create_time,
                    NOW() AS update_time  
            from kpi_po_mx 
            where item_code IN('2.1','2.2')
            group by month,market_id,market_name""".format(get_bbrq())
    cr.execute(st)
    db.commit()

    # calc region gmv/sales
    st ="""
    INSERT INTO kpi_po_mx(bbrq,MONTH,market_id,market_name,item_code,item_name,item_value,item_value_sum,create_time,update_time)
            SELECT 
                 bbrq,
                 MONTH,
                 market_id,
                 market_name,
                '10' AS item_code,
                '线上GMV/线下销售额' AS item_name,
                ROUND((SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value,
                ROUND((SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value_sum,
                NOW() AS create_time,
                    NOW() AS update_time  
            FROM kpi_po_mx a WHERE a.month='{}' AND a.market_id=20000 AND a.item_code='9'
            UNION ALL
            SELECT 
                 bbrq,
                 MONTH,
                 market_id,
                 market_name,
                '10' AS item_code,
                '线上GMV/线下销售额' AS item_name,
                ROUND((SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value,
                ROUND((SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value_sum,
                NOW() AS create_time,
                    NOW() AS update_time  
            FROM kpi_po_mx a WHERE a.month='{}' AND a.market_id=30000 AND a.item_code='9'
            UNION ALL 
            SELECT 
                 bbrq,
                 MONTH,
                 market_id,
                 market_name,
                '10' AS item_code,
                '线上GMV/线下销售额' AS item_name,
                ROUND((SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value,
                ROUND((SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='2')/
                      (SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='9'),2) AS item_value_sum,
                NOW() AS create_time,
                    NOW() AS update_time  
            FROM kpi_po_mx a WHERE a.month='{}' AND a.market_id=40000 AND a.item_code='9'
                """.format(get_month(g_month),get_month(g_month),get_month(g_month))
    cr.execute(st)
    db.commit()

    # calc hst gmv
    st = """INSERT INTO kpi_po_mx(bbrq,MONTH,market_id,market_name,item_code,item_name,item_value,item_value_sum,create_time,update_time)
                     SELECT '{}',
                           MONTH,
                           market_id,
                           market_name,
                           '12' AS item_code,
                           '总GMV指标（万）' AS item_name,
                           round(SUM(item_value+0),2) AS item_value,
                           round(SUM(item_value_sum+0),2) AS item_value_sum,
                           NOW() AS create_time,
                           NOW() AS update_time  
                     FROM kpi_po_mx 
                     WHERE item_code IN('12.1','12.2')
                     GROUP BY MONTH,market_id,market_name""".format(get_bbrq())
    cr.execute(st)
    db.commit()

    # 计算 HST GMV/线下销售额
    st = """
      INSERT INTO kpi_po_mx(bbrq,MONTH,market_id,market_name,item_code,item_name,item_value,item_value_sum,create_time,update_time)
          SELECT 
               bbrq,
               MONTH,
               market_id,
               market_name,
              '15' AS item_code,
              '线上GMV/线下销售额' AS item_name,
              ROUND((SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='12')/
                    (SELECT item_value FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='13'),2) AS item_value,
              ROUND((SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='12')/
                    (SELECT item_value_sum FROM kpi_po_mx b WHERE a.month=b.month AND a.market_id=b.market_id AND b.item_code='13'),2) AS item_value_sum,
              NOW() AS create_time,
                  NOW() AS update_time  
          FROM kpi_po_mx a WHERE a.month='{}' AND a.market_id=90000 AND a.item_code='12'          
          """.format(get_month(g_month))
    cr.execute(st)
    db.commit()

def update_hz(db):
    cr = db.cursor()
    if check_hz_exists(db) == 0:
        print('insert kpi_po_hz log....')
        st = """INSERT INTO `kpi_po_hz`(bbrq,MONTH,market_id,market_name,item_code,item_name,actual_completion,completion_sum_finish,create_time)
                  SELECT bbrq,MONTH,market_id,market_name,item_code,item_name,item_value,item_value_sum,NOW()
                  FROM kpi_po_mx
                  WHERE MONTH='{}'""".format(get_month(g_month))

    else:
        print('update kpi_po_hz log....')
        st ="""UPDATE kpi_po_hz a
                      SET actual_completion=(SELECT item_value FROM kpi_po_mx b
                                              WHERE b.month=a.month 
                                                AND b.market_id=a.market_id 
                                                AND b.item_code=a.item_code),
                          completion_sum_finish=(SELECT item_value_sum FROM kpi_po_mx b 
                                                 WHERE b.month=a.month 
                                                   AND b.market_id=a.market_id 
                                                   AND b.item_code=a.item_code),
                          update_time=NOW(),
                          bbrq=(SELECT bbrq FROM kpi_po_mx b
                                              WHERE b.month=a.month 
                                                AND b.market_id=a.market_id 
                                                AND b.item_code=a.item_code)
                    WHERE MONTH='{}'""".format(get_month(g_month))
    cr.execute(st)
    db.commit()

if __name__ == '__main__':
    db     = get_db()
    dbd    = get_db_dict()
    bbrqq  = get_first_day(g_month)
    ybbrqq = get_first_day_year()
    bbrqz  = get_last_day(g_month)

    print('统计范围:')
    print('-------------------------------------------')
    print('  本月 :{}~{}'.format(bbrqq,bbrqz))
    print('  累计 :{}~{}'.format(ybbrqq, bbrqz))
    print('-------------------------------------------')

    # 生成各项目统计数据
    for market in get_markets(dbd,'N'):
        bb = get_bbtj_sql(dbd)
        print("Calculating project {}...".format(market['market_name']))
        for b in bb:
            if b['if_stat'] == 'Y':
                b['statement'] = b['statement'].replace('$$BBRQQ$$',bbrqq).replace('$$BBRQZ$$',bbrqz).replace('$$MARKET_ID$$',str(market['market_id']))
                b['statement_sum'] = b['statement_sum'].replace('$$BBRQQ$$', bbrqq).replace('$$BBRQZ$$', bbrqz).replace('$$MARKET_ID$$', str(market['market_id']))
            b['value'],b['value_sum']  = get_value(dbd,b['ds_id'],b['statement'],b['statement_sum'])
            b['market_id'] = market['market_id']
            b['market_name'] = market['market_name']

        # 写日志
        write_log(dbd,bb)

    # 生成区域汇总数据
    for market in get_markets(dbd, 'Y'):
        bb = get_bbtj_sql(dbd)
        print("Calculating region {}...".format(market['market_name']))
        for b in bb:
            if b['if_stat'] == 'Y':
                b['statement'] = b['statement'].replace('$$BBRQQ$$', bbrqq)\
                                               .replace('$$BBRQZ$$', bbrqz)\
                                               .replace('$$MARKET_ID$$', market['summary_formula'])
                b['statement_sum'] = b['statement_sum'].replace('$$BBRQQ$$', bbrqq)\
                                                       .replace('$$BBRQZ$$', bbrqz)\
                                                       .replace('$$MARKET_ID$$', market['summary_formula'])
            b['value'], b['value_sum'] = get_value(dbd, b['ds_id'], b['statement'], b['statement_sum'])
            b['market_id'] = market['market_id']
            b['market_name'] = market['market_name']

        # 写日志
        write_log(dbd,bb)

    # 生成总部汇总数据
    for market in get_markets(dbd, 'HST'):
        bb = get_bbtj_sql_hst(dbd)
        print("Calculating hst {}...".format(market['market_name']))
        for b in bb:
            if b['if_stat'] == 'Y':
                b['statement'] = b['statement'].replace('$$BBRQQ$$', bbrqq).replace('$$BBRQZ$$', bbrqz)
                b['statement_sum'] = b['statement_sum'].replace('$$BBRQQ$$', bbrqq).replace('$$BBRQZ$$', bbrqz)
            b['value'], b['value_sum'] = get_value(dbd, b['ds_id'], b['statement'], b['statement_sum'])
            b['market_id'] = market['market_id']
            b['market_name'] = market['market_name']

        # 写日志
        write_log(dbd, bb)

    # 更新合计项
    update_log(dbd)

    # 更新 kpi_po_hz
    update_hz(dbd)

    # 生成HTML
    v_title = '商管BI-KPI统计情况表-{}'.format(get_bbrq())
    v_content = get_html_contents(dbd)

    # 发送邮件
    send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_content)
    # send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8','190343@lifeat.cn,190634@lifeat.cn,807216@lifeat.cn',v_title, v_content)

    # 释放连接
    db.close()
    dbd.close()
