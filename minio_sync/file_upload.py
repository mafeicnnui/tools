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

#上海五角场图片路径：/home/hopson/apps/var/webapps/JS_images

config= {
    'file_path':'/home/hopson/apps/usr/webserver/JS_images/',
    'bucket_name':'110'
}

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


def upload_file():
    '''
       1.建立bucket ，遍历file_path路径下所有子目录  pip install minio
    '''
    try:
        minioClient.make_bucket(config['bucket_name'], location="cn-north-1")
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        pass
    except ResponseError as err:
        pass
    print('Bucket {0} created!'.format(config['bucket_name']))

    '''
       2.全量上传文件至bucket
    '''
    start_time = datetime.datetime.now()
    for root, dirs, files in os.walk(config['file_path']):

        if len(files)>0:
            for file in files:
                try:
                    full_name = root + '/' + file
                    print('\rUploading file: {0} into bucket {1},Elaspse Time:{2}s'.
                          format(file,config['bucket_name'],get_seconds(start_time)), end='')
                    minioClient.fput_object(config['bucket_name'],
                                            full_name.replace(config['file_path'],''),
                                            full_name,
                                            'image/jpeg')
                except ResponseError as err:
                    print(err)


if __name__ == "__main__":
     upload_file()