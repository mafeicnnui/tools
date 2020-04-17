#!E:/devops/Python27/python
# -*- coding: utf-8 -*-
# liuliguo 20180224

import smtplib
import time  
from email.mime.text import MIMEText  
from email.mime.multipart import MIMEMultipart  
from email.header import Header  
from email import encoders  
from email.mime.base import MIMEBase  
from email.utils import parseaddr, formataddr
import codecs
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# 格式化邮件地址  
def formatAddr(s):  
	name, addr = parseaddr(s)  
	return formataddr((Header(name, 'utf-8').encode(), addr))

def sendMail(title,body, attachment):  
	smtp_server = 'smtp.winchannel.net'  
	from_mail = 'maintenance1@winchannel.net'  
	mail_pass = 'winchannel1234'  
	to_mail = ['mafei02@winchannel.net']
	
	# 构造一个MIMEMultipart对象代表邮件本身  
	msg = MIMEMultipart()  
	# Header对中文进行转码  
	msg['From'] = formatAddr('maintenance1 <%s>' % from_mail).encode()  
	msg['To'] = ','.join(to_mail)  
	h = title
	msg['Subject'] = Header(h, 'utf-8').encode()  
	# plain代表纯文本  
	msg.attach(MIMEText(body, 'html', 'utf-8'))  
	# 二进制方式模式文件	
	with open(attachment, 'rb') as f:  
		# MIMEBase表示附件的对象  
		mime = MIMEBase('text', 'txt', filename=attachment)  
		# filename是显示附件名字  
		mime.add_header('Content-Disposition', 'attachment', filename=attachment)  
		# 获取附件内容  
		mime.set_payload(f.read())				
		encoders.encode_base64(mime) 
		# 作为附件添加到邮件  
		msg.attach(mime)
	#msg.attach(MIMEText(v_html, 'html', 'utf-8'))
	try:  
		s = smtplib.SMTP()  
		s.connect(smtp_server, "80")  
		s.login(from_mail, mail_pass)  
		s.sendmail(from_mail, to_mail, msg.as_string())  # as_string()把MIMEText对象变成str  
		s.quit()  
	except smtplib.SMTPException as e:  
		print "Error: %s" % e
		
if __name__ == "__main__":   
	v_title_gbk='voltaren项目MySQL数据库备份情况1'.encode("gbk")
	v_title_utf8='voltaren项目MySQL数据库备份情况1'.encode("utf8")
	print v_title_gbk
	line=''
	with open('mail_content.html', 'r') as f:  
	  line=line+str(f.read())	
	#sendMail(v_title_utf8, line.decode('gbk'),'mail_content.html')
	sendMail(v_title_utf8, line.decode('gbk'),'mail_content.html')

