#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/8/3 8:35
# @Author : 马飞
# @File : slow_log_cut.py.py
# @Software: PyCharm

'''
  1.每天一个慢日志文件
  2.每天0点开始切割
  3.每隔3分钟分析一次慢日志，调用接口回传至元数据库，做为一个事务进行操作
'''

