#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import sys
import traceback
import os
import configparser
import time
import datetime
import smtplib
import pymysql
from email.mime.text import MIMEText

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):    
    to_user=p_to_user.split(",")   
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)       
        server = smtplib.SMTP("smtp.winchannel.net", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit() 
    except smtplib.SMTPException as e:        
        print(e)
		
def main():
   send_user    = "maintenance1@winchannel.net"
   send_pass    = "winchannel1234"
   acpt_user    = "its@winchannel.net,luobin@winchannel.net,zhangjianqiang@winchannel.net,mazhiqiang@winchannel.net"
   #acpt_user    = "mafei02@winchannel.net"
   mail_title   = sys.argv[1]
   mail_content = sys.argv[2]
   #print("mail_title2=",mail_title)
   #print("mail_content2=",mail_content)   
   send_mail(send_user,send_pass,acpt_user,mail_title,mail_content)
  
main()  
