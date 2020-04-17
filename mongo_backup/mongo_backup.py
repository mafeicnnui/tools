#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
import sys,time
import configparser
import warnings
import pymongo
import os
import datetime
import smtplib
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
    except smtplib.SMTPException as e:
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

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mongo(ip,port,replset):
    conn = pymongo.MongoClient(host=ip, port=int(port),replicaSet=replset)
    return conn

def get_ds_mongo(ip,port):
    conn = pymongo.MongoClient(host=ip, port=int(port))
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    #get mail parameter
    config['send_user']                = cfg.get("sync", "send_mail_user")
    config['send_pass']                = cfg.get("sync", "send_mail_pass")
    config['acpt_user']                = cfg.get("sync", "acpt_mail_user")
    config['mail_title']               = cfg.get("sync", "mail_title")
    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_replset                   = db_mongo.split(':')[2]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_replset']         = db_mongo_replset
    #config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port,db_mongo_replset)
    config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port)
    config['mongodump']                = cfg.get("sync", "mongodump")
    config['backup_path']              = cfg.get("sync", "backup_path")
    return config




def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

def get_html_contents(tbody,path):
    thead  = '''<tr><td width=10%>库名</td>
                    <td width=10%>文件名</td>
                    <td width=10%>文件大小</td>
                    <td width=10%>备份耗时(s)</td>
                    <td width=10%>完成情况</td>
               </tr>'''
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
                     <h4>MongoDB备份情况：</h4>
                     <h4>备份路径:'''+path+'''</h4>
                      <table class="xwtable">
                         <thead>\n''' + thead + '\n</thead>\n' + '''
                         <tbody>\n''' + tbody + '\n</tbody>\n' + '''
                      </table>
                </body>
              </html>
           '''
    return v_html

def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #初始化
    config = init(config, debug)

    #获取连接对象
    db_mongodb = config['db_mongo']

    #获取数据名列表
    db_list=db_mongodb.list_database_names()
    v_temp = '''<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td></tr>'''
    v_path =config['db_mongo_ip']+':'+config['backup_path']+'/'+get_date()
    v_body =''
    os.system('mkdir -p {0}'.format(config['backup_path']+'/'+get_date()))
    for dname in db_list:
        cmd="{0} -h {1}:{2} -d {3} -o {4}/{5}/ &>/tmp/mongo_backup.log".\
            format(config['mongodump'],config['db_mongo_ip'],
                   config['db_mongo_port'],dname,config['backup_path'],get_date())
        if dname not in('admin','local','push'):
           start_time = datetime.datetime.now()
           print('backup database {0}...'.format(dname))
           result=os.system(cmd)
           print('backup database {0} complet,elapse:{1}s...'.format(dname,str(get_seconds(start_time))))
           if result==0:
              filename=dname+'.tar.gz'
              fullname=config['backup_path']+'/'+get_date()+'/'+filename
              os.system('cd {0} && tar czf {1} {2}'.format(config['backup_path']+'/'+get_date(),filename,dname))
              os.system('rm -rf {0}'.format(config['backup_path']+'/'+get_date()+'/'+dname))
              filesize=os.path.getsize(fullname)
              print(filename,fullname,filesize)
              v_body=v_body+v_temp.format(dname,filename,filesize,str(get_seconds(start_time)),'成功！')
           else:
              v_body=v_body+v_temp.format(dname,filename,filesize,str(get_seconds(start_time)),'失败！')

    send_mail465(config['send_user'], config['send_pass'], config['acpt_user'], config['mail_title'],
                 get_html_contents(v_body,v_path))
    print('mail send success!')
if __name__ == "__main__":
     main()

