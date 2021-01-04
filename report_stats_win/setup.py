#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = '马飞'

from cx_Freeze import setup, Executable

setup(name='weekstats_ops',
      version='1.0',
      description='商管周报生成工具',
      executables=[Executable("weekstats_ops.py")]
      )
