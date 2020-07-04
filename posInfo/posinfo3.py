#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/5/21 11:00
# @Author : 马飞
# @File : posinfo.py.py
# @func : download picture
# @Software: PyCharm

import requests
from io import BytesIO
from PIL import Image
from dateutil import parser
import base64
import pymongo
import datetime

'''
  功能：接口配置
'''
config = {
    "interface_get_image": "http://39.106.206.36:8080/posbox/viewTicket",
    "interface_get_image_result": "http://39.106.206.36:8080/posbox/viewExtractResult",
    "interface_get_image_ocr_result": "http://39.106.206.36:8080/posbox/viewOCRResult",
    "mongo_string": "39.106.184.57:27016:posB:root:JULc9GnEuNHYUTBG",
    "limits": "50",         # 每个店铺提取数据笔数
    "marketId": "bjcyhsh",  # 项目编码:北京朝合:bjcyhsh,上海五角场:shhsh
    "ticketType": "1-3"     # 小票编号前缀1-3为图片，4为文本
}

'''
  功能：获取时间差：单位:秒
'''
def get_seconds(b):
    a = datetime.datetime.now()
    return int((a - b).total_seconds())


def get_market_name(p_market_id):
    if p_market_id == 'bjcyhsh':
        return '北京朝阳合生汇'
    elif p_market_id == 'shhsh':
        return '上海五角场合生汇'
    else:
        return ''


'''
  功能：获取小票图片写图片文件
'''
def write_image(cfg):
    url = '{}?marketId={}&posId={}&ticketNo={}'\
        .format(cfg['interface_get_image'], cfg['marketId'], cfg['posId'],cfg['ticketNo'])
    try:
        file = '{}_{}_{}.png'.format(cfg['marketId'], cfg['posId'], cfg['ticketNo'])
        response = requests.get(url)
        obj = BytesIO(response.content)
        img = Image.open(obj)
        img.save('./images/' + file, 'PNG')
        #print('Write Image:{} ok!'.format(file))
    except:
        pass


'''
  功能：获取小票图片base64编码
'''


def get_image_base64(cfg):
    url = '{}?marketId={}&posId={}&ticketNo={}'.format(cfg['interface_get_image'], cfg['marketId'], cfg['posId'],
                                                       cfg['ticketNo'])
    response = requests.get(url)
    in_buf = BytesIO(response.content)
    out_buf = BytesIO()
    base64.encode(in_buf, out_buf)
    return out_buf.getvalue().decode('utf-8')


'''
  功能：获取小票类型

'''


def get_ticket_type(cfg):
    if cfg['ticketNo'].split('_')[0] == '400':
        return '文本'
    else:
        return '图片'


'''
  功能：获取接口返回状态
  状态：{errorCode:0,data:"”}:data后是OCR识别结果
       {errorCode:-1,errorMessage:参数异常}
       {errorCode:-2,errorMessage:参数为空}
       {errorCode:-3,errorMessage:文件不存在}
       {errorCode:-4,errorMessage:服务器运行异常
'''


def get_ticket_status(errcode):
    result = ''
    if errcode == '0':
        result = '调用成功'
    elif errcode == '-1':
        result = '参数异常'
    elif errcode == '-2':
        result = '参数为空'
    elif errcode == '-3':
        result = '文件不存在'
    elif errcode == '-4':
        result = '服务器运行异常'
    else:
        pass
    return result


'''
  功能：获取小票图片提取结果,返回提取状态和结果
'''


def get_ticket_image(cfg):
    url = '{}?marketId={}&posId={}&ticketNo={}&type=1'.format(cfg['interface_get_image_result'], cfg['marketId'],
                                                              cfg['posId'], cfg['ticketNo'])
    response = requests.post(url)
    errorCode = response.text.replace('{', '').replace('}', '').split(',')[0].split('errorCode')[1][1:]
    data = response.text.replace('{', '').replace('}', '').split(',')[1].split('data')[1][1:]
    result = '返回状态:{}<br>{}'.format(get_ticket_status(errorCode), data)
    return result


'''
  功能：获取小票图片ocr识别结果
'''


def get_ticket_image_ocr(cfg):
    url = '{}?marketId={}&posId={}&ticketNo={}&type=1'.format(cfg['interface_get_image_ocr_result'], cfg['marketId'],
                                                              cfg['posId'], cfg['ticketNo'])
    response = requests.post(url)
    errorCode = response.text.replace('{', '').replace('}', '').split(',')[0].split('errorCode')[1][1:]
    data = response.text.replace('{', '').replace('}', '').split(',')[1].split('data')[1][1:]
    result = '返回状态:{}<br>{}'.format(get_ticket_status(errorCode), data)
    return result

'''
  功能：连接mongodb
'''
def get_ds_mongo_auth(mongodb_str):
    ip = mongodb_str.split(':')[0]
    port = mongodb_str.split(':')[1]
    service = mongodb_str.split(':')[2]
    user = mongodb_str.split(':')[3]
    password = mongodb_str.split(':')[4]
    conn = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip, int(port)))
    db = conn[service]
    db.authenticate(user, password)
    return db


'''
  功能：格式化输出字典
'''
def print_dict(config):
    print('-'.ljust(150, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(40, ' ') + 'value')
    print('-'.ljust(125, '-'))
    for key in config:
        if key == 'sync_table':
            print(' '.ljust(3, ' ') + key.ljust(40, ' ') + '=', config[key])
        else:
            print(' '.ljust(3, ' ') + key.ljust(40, ' ') + '=', config[key])
    print('-'.ljust(125, '-'))


'''
  功能：动态生成mongo查询条件
'''
def get_mongo_where(cfg):
    v_rqq = parser.parse('2020-05-31T00:00:00.000Z')
    v_rqz = parser.parse('2020-06-09T23:59:59.000Z')
    v_json = {"$and": [
        {"updateTime": {"$gte": v_rqq, "$lte": v_rqz}},
        {"marketId": cfg['marketId']},
        {"ticketNo": {"$regex": "^[{}]".format(cfg['ticketType'])}}
    ]
    }
    print(v_json)
    return v_json


'''
  功能：输出基本信息
'''
def get_info(cfg):
    v_result = '''
<strong>商场代码</strong>   :{}<br>  
<strong>商场名称</strong>   :{}<br>   
<strong>店铺名称 </strong>  :{}<br>
<strong>合生通编码</strong> :{}<br>
<strong>合生通业态</strong> :{}<br>
<strong>小票编号</strong>   :{}<br>
<strong>小票类型 </strong>  :{}<br>
'''.format(cfg['marketId'],
           get_market_name(cfg['marketId']),
           cfg['shop_name'],
           cfg['shop_code'],
           cfg['shop_type'],
           cfg['ticketNo'],
           get_ticket_type(cfg)
           )
    return v_result


'''
  功能：根据配置生成html
'''
def get_images(config):
    start = datetime.datetime.now()
    v_where = get_mongo_where(config)
    v_result = {"marketId": 1, "shopId": 1, "posId": 1, "ticketNo": 1}
    mongo_cur = config['mongo_db']['saleDetail']
    results = mongo_cur.find(v_where, v_result)
    n_total = results.count()
    i_counter = 0
    print('Find {} pictures...'.format(n_total))
    for rec in results:
        config['posId'] = rec['posId']
        config['ticketNo'] = rec['ticketNo']
        write_image(config)
        i_counter = i_counter +1
        print('\rGeneating picture {}%'.format(round(i_counter / n_total * 100, 2)), end='')
    print('Geneating {} picture  ok!'.format(config['marketId']))
    print('Elaspse Time: {}s'.format(get_seconds(start)))


'''
    功能：猫酷销售小票信息图片生成
'''
if __name__ == "__main__":
    config['mongo_db'] = get_ds_mongo_auth(config.get("mongo_string"))

    # 打印配置信息
    print_dict(config)

    # 生成朝合店铺小票html
    config['marketId'] = 'bjcyhsh'
    get_images(config)
