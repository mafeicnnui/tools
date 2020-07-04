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

'''
  功能：全局配置
'''
config = {
    "chat_interface":"https://alarm.lifeat.cn/wx/cp/msg/1000015",
    "mysql":"rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com:3306:shop_side_operation_sg:hopsononebi_2019:xMEAnLk8SOfa8bEwq2Xl6LpuI8QWXb0",
    "warn_level":"紧急"
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


'''
 功能：获取数据库连接
'''
def get_config(config):
    db_ip                   = config['mysql'].split(':')[0]
    db_port                 = config['mysql'].split(':')[1]
    db_service              = config['mysql'].split(':')[2]
    db_user                 = config['mysql'].split(':')[3]
    db_pass                 = config['mysql'].split(':')[4]
    config['db_mysql']      = get_ds_mysql(db_ip,db_port,db_service,db_user,db_pass)
    config['db_mysql_dict'] = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
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
 功能：获取server信息
'''
def get_server_info(config,server_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = 'SELECT * FROM t_server where id={}'.format(server_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def str2datetime(p_rq):
    return datetime.datetime.strptime(p_rq, '%Y-%m-%d %H:%M:%S')

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
    elif p_market_id=='207':
        return '北京德胜合生财富广场'
    elif p_market_id=='183':
        return '上海海云天合生财富广场'
    else:
        return ''


def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


def call_proc(cfg):
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    start_time = datetime.datetime.now()
    print("call shop_side_operation_sg.proc_sales_report_day_total procedure please wait...")
    cr.callproc('shop_side_operation_sg.proc_sales_report_day_total')
    db.commit()
    cr.close()
    print('call shop_side_operation_sg.proc_sales_report_day_total procedure complete,elaspse:{0}s'.format(str(get_seconds(start_time))))

def send_webchat(cfg):
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    ft = '''
商场名称：{}
交易日期：{}
审核通过销售金额：{}
审核通过销售笔数：{}
审核通过条数：{}
生成时间：{}
发送时间：{}
'''

    cr.execute("""SELECT market_id,
                             trade_date,
                             total_Sales_Amount,
                             totalTradeCount,
                             actualReport,
                             date_format(create_date,'%Y-%m-%d %H:%i:%S') as  create_date
                       FROM shop_side_operation_sg.sales_report_day_sum
                      WHERE trade_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                        ORDER BY trade_date DESC """)
    rs = cr.fetchall()
    for r in rs:
        v_title   = '商管-日销售额统计报表[{}]'.format(r['market_id'])
        v_content = ft.format(
            get_market_name(str(r['market_id'])),
            r['trade_date'],
            r['total_Sales_Amount'],
            r['totalTradeCount'],
            r['actualReport'],
            r['create_date'],
            get_time()
        )
        send_message(cfg, v_title + v_content)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()


if __name__ == "__main__":
    cfg = get_config(config)
    call_proc(cfg)
    send_webchat(cfg)
