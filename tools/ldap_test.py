#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/6/3 19:55
# @Author : 马飞
# @File : ldap_test.py
# @Software: PyCharm


'''
sudo yum -y install build-essential python3-dev python2.7-dev libldap2-dev libsasl2-dev slapd ldap-utils python-tox lcov valgrind
sudo yum groupinstall "Development tools"
sudo yum install openldap-devel python-devel
pip3 install ldap3

'''

import ldap3
from ldap3 import Server, Connection, ALL

server = Server('10.2.39.16', get_info=ALL)
conn   = Connection(server, 'cn=Manager, dc=ldap, dc=com','hopson@2019', auto_bind=True)
conn.search('cn= mafei,ou=People,dc=ldap,dc=com', '(&(uid=mafei))', attributes=['userPassword'])
print('conn.entries[0]=',conn.entries[0])
entry = conn.entries[0]
userpass=entry.userPassword

print('type(userpass)=',type(userpass),userpass.values)
print(type(userpass.values),userpass.values)

print('type(userpass)=',type(userpass),userpass.value)
print(type(userpass.value),userpass.value,str(userpass.value, encoding="utf-8"))
