#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/4/23 10:06
# @Author : 马飞
# @File : file_upload.py
# @Software: PyCharm
# @func:.项目图片文件同步至miniO服务器

import os
import datetime
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)
minioClient = Minio('10.2.39.50:30004',access_key='minio',secret_key='minio123',secure=False)


config= {

    'file_path':'/home/hopson/apps/usr/webserver/JS_images/',
    'bucket_name':'110'
}

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


def download_file():
    try:
        minioClient.fget_object(config['bucket_name'], '20200510/0000/C000 苏-AL08A20000_CRD000000_20200510000459542_4号入口_01_SB.jpg', 'cs.png')
    except ResponseError as err:
        print(err)

if __name__ == "__main__":
    download_file()