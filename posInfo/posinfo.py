#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/5/21 11:00
# @Author : 马飞
# @File : posinfo.py.py
# @Software: PyCharm

import requests
from   io import BytesIO
from   PIL import Image
from   dateutil import parser
import base64
import pymongo
import datetime


'''
  功能：接口配置
'''
config = {
    "interface_get_image":"http://39.106.206.36:8080/posbox/viewTicket",
    "interface_get_image_result":"http://39.106.206.36:8080/posbox/viewExtractResult",
    "interface_get_image_ocr_result":"http://39.106.206.36:8080/posbox/viewOCRResult",
    "mongo_string":"39.106.184.57:27016:posB:root:JULc9GnEuNHYUTBG",
    "limits":"10",        #每个店铺提取数据笔数
    "marketId":"bjcyhsh", #项目编码:北京朝合:bjcyhsh,上海五角场:shhsh
    "ticketType":"1-4"    #小票编号前缀1-3为图片，4为文本
}


'''
  功能：北京朝合店铺映射
'''
shopMap_218 = {
    "2616": "美食餐饮:一味一诚:5a2ca21c890a46683d791524",
    "2468": "美食餐饮:满记甜品:5a2e3f7d890a46683d79155f",
    "2485": "美食餐饮:渡娘火锅:5a2cab38e50042202448d3a8",
    "2591": "美食餐饮:胖哥俩肉蟹煲:5a2ca9f1890a46683d79153b",
    "2655": "美食餐饮:杨记兴臭鳜鱼:5a2cf4a1e50042202448d3b7",
    "2609": "美食餐饮:汤城小厨:5a2ca9a8e50042202448d3a4",
    "2607": "美食餐饮:塔哈尔新疆盛宴:5a2caa27890a46683d79153e",
    "3029": "美食餐饮:无敌家:5a40bca195e32a6005f9dc1a",
    "2969": "美食餐饮:米芝莲+宇治茶铺:5a1e64cc95e32a062a56bbf5",
    "3131": "美食餐饮:争鲜:5a320eb3890a46683d79158b",
    "4165": "品牌服饰:马克华菲:5a1e65f695e32a062a56bbfc",
    "2649": "品牌服饰:马克华菲（女装）:5a1ffdad95e32a112bc0c29f",
    "2488": "品牌服饰:VERO MODA:5a0a78cd890a46754afc06de",
    "2490": "品牌服饰:play lounge:5a2ca05d95e32a6005f9db7a"
}


'''
  功能：上海五角场店铺映射
'''
shopMap_110 = {
    "269"  :"美食餐饮:和府捞面:57df9cc40cf23a8551fa1009",
    "705"  :"美食餐饮:潮记1975点心专门店:5d3bc0bd2eec556cafa8f9fa",
    "530"  :"美食餐饮:曼哈顿海鲜自助餐:586ccce70cf297d776e33685",
    "573"  :"美食餐饮:皖香小菜园:5bffbc047670f77e3b22237e",
    "2971" :"美食餐饮:青花椒:59dee4bb0cf2f9b34144ddb0",
    "5680" :"美食餐饮:维记:5c7529237670f76ecc3e6e5b",
    "5693" :"美食餐饮:海野家舞渔:5d3bc7f52eec556cafa91254",
    "6303" :"美食餐饮:韩宫宴:5c6ba7d87670f73f600d051d",
    "6357" :"美食餐饮:家府潮汕菜:5cc510eb7670f75e8cce7d52",
    "16287":"美食餐饮:极火酱烧:5d1c4f0c7670f75f5abb21ce",
    "16346":"品牌服饰:YOUNGOR:5d26d4012eec5573f5a57dcd",
    "16398":"品牌服饰:ABUN:5d68c6532eec552df266dbb1",
    "16401":"品牌服饰:YARVE:5d6cdb042eec555763188e24",
    "16403":"品牌服饰:Inxx:5d6cda312eec555763188c54"
}

'''
  功能：获取时间差：单位:秒
'''
def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_market_name(p_market_id):
    if p_market_id=='bjcyhsh':
       return '北京朝阳合生汇'
    elif p_market_id=='shhsh':
       return '上海五角场合生汇'
    else:
       return ''

'''
  功能：获取小票图片写图片文件
'''
def write_image(cfg):
    url      = '{}?marketId={}&posId={}&ticketNo={}'.format(cfg['interface_get_image'],cfg['marketId'],cfg['posId'],cfg['ticketNo'])
    file     = '{}_{}_{}.png'.format(cfg['marketId'],cfg['posId'],cfg['ticketNo'])
    response = requests.get(url)
    obj      = BytesIO(response.content)
    img      = Image.open(obj)
    img.save(file, 'PNG')
    print('Write Image:{} ok!'.format(file))

'''
  功能：获取小票图片base64编码
'''
def get_image_base64(cfg):
    url      = '{}?marketId={}&posId={}&ticketNo={}'.format(cfg['interface_get_image'],cfg['marketId'],cfg['posId'],cfg['ticketNo'])
    response = requests.get(url)
    in_buf   = BytesIO(response.content)
    out_buf  = BytesIO()
    base64.encode(in_buf,out_buf)
    return out_buf.getvalue().decode('utf-8')

'''
  功能：获取小票类型
  
'''
def get_ticket_type(cfg):
    if cfg['ticketNo'].split('_')[0]=='400':
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
    result =''
    if errcode =='0':
       result ='调用成功'
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
    url         = '{}?marketId={}&posId={}&ticketNo={}&type=1'.format(cfg['interface_get_image_result'],cfg['marketId'],cfg['posId'],cfg['ticketNo'])
    response    = requests.post(url)
    errorCode   = response.text.replace('{','').replace('}','').split(',')[0].split('errorCode')[1][1:]
    data        = response.text.replace('{','').replace('}','').split(',')[1].split('data')[1][1:]
    result      = '返回状态:{}<br>{}'.format(get_ticket_status(errorCode),data)
    return result


'''
  功能：获取小票图片ocr识别结果
'''
def get_ticket_image_ocr(cfg):
    url         = '{}?marketId={}&posId={}&ticketNo={}&type=1'.format(cfg['interface_get_image_ocr_result'],cfg['marketId'],cfg['posId'],cfg['ticketNo'])
    response    = requests.post(url)
    errorCode   = response.text.replace('{','').replace('}','').split(',')[0].split('errorCode')[1][1:]
    data        = response.text.replace('{','').replace('}','').split(',')[1].split('data')[1][1:]
    result      = '返回状态:{}<br>{}'.format(get_ticket_status(errorCode),data)
    return result

'''
  功能：获取小票文本识别结果
'''
def get_ticket_text(cfg):
    pass

'''
  功能：连接mongodb
'''
def get_ds_mongo_auth(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    db.authenticate(user, password)
    return db

'''
  功能：格式化输出字典
'''
def print_dict(config):
    print('-'.ljust(150,'-'))
    print(' '.ljust(3,' ')+"name".ljust(40,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        if key=='sync_table':
           print(' '.ljust(3, ' ') + key.ljust(40, ' ') + '=',config[key])
        else:
           print(' '.ljust(3,' ')+key.ljust(40,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

'''
  功能：动态生成mongo查询条件
'''
def get_mongo_where(cfg):
    v_rqq = parser.parse('2020-01-01T00:00:00.000Z')
    v_rqz = parser.parse('2020-04-30T23:59:59.000Z')
    v_json = {"$and":[
         {"updateTime": {"$gte": v_rqq, "$lte": v_rqz}},
         {"marketId"  : cfg['marketId']},
         {"shopId"    : cfg['shop_id']},
         {"ticketNo"  : {"$regex": "^[{}]".format(cfg['ticketType'])}}
       ]
    }
    return v_json

'''
  功能：输出基本信息
'''
def get_info(cfg):
   v_result ='''
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
def gen_html(cfg,shops):
    start  = datetime.datetime.now()
    v_html = '''
    <!DOCTYPE html>
    <html>
        <head>
             <meta charset="utf-8">
             <link href="http://libs.baidu.com/bootstrap/3.0.3/css/bootstrap.min.css" rel="stylesheet">
             <style>
               hr {
                  -moz-border-bottom-colors: none;
                  -moz-border-image: none;
                  -moz-border-left-colors: none;
                  -moz-border-right-colors: none;
                  -moz-border-top-colors: none;
                  border-color: #EEEEEE -moz-use-text-color #FFFFFF;
                  border-style: solid none;
                  border-width: 1px 0;
                  margin: 18px 0;
                }
             </style>
        </head>	
        <body>
          $$IMAGE$$
          <script src="http://libs.baidu.com/jquery/2.0.0/jquery.min.js"></script>
          <script src="http://libs.baidu.com/bootstrap/3.0.3/js/bootstrap.min.js"></script>
        </body>
    </html>
    '''
    v_image_templete = '''
            <div class="row">
                <form class="form-horizontal" role="form">
                   <div class="col-md-3"><span class="text-center"><strong>基本信息</strong></span></div>
                   <div class="col-md-3"><span class="text-center"><strong>小票图片</strong></span></div>
                   <div class="col-md-3"><span class="text-center"><strong>小票图片提取结果</strong></span></div>
                   <div class="col-md-3"><span class="text-center"><strong>小票OCR识别结果</strong></span></div>
                 </form>
           </div>
           <hr/>
           <div class="row">
                <form class="form-horizontal" role="form">
                   <div class="col-md-3">$$INFO$$</div>
                   <div class="col-md-3"><img width="70%" src="data:image/png;base64,$$CONTENT$$"/></div>
                   <div class="col-md-3">$$IMG_CONTENT$$</div>
                   <div class="col-md-3">$$OCR_CONTENT$$</div>
                 </form>
           </div>
    '''
    v_all_image  = ''
    for key in shops:
        config['shop_type'] = shops[key].split(':')[0]
        config['shop_code'] = key
        config['shop_name'] = shops[key].split(':')[1]
        config['shop_id']   = shops[key].split(':')[2]
        v_where             = get_mongo_where(config)
        v_result            = {"marketId": 1, "shopId": 1, "posId": 1, "ticketNo": 1}
        mongo_cur           = config['mongo_db']['saleDetail']
        results             = mongo_cur.find(v_where, v_result).limit(int(config['limits']))
        v_shop_image        = ''
        for rec in results:
            config['posId'] = rec['posId']
            config['ticketNo'] = rec['ticketNo']
            info        = get_info(config)
            base64_str  = get_image_base64(config)
            image_str   = get_ticket_image(config)
            ocr_str     = get_ticket_image_ocr(config)
            v_image     = v_image_templete. \
                           replace('$$INFO$$', info). \
                           replace('$$CONTENT$$', base64_str). \
                           replace('$$IMG_CONTENT$$', image_str). \
                           replace('$$OCR_CONTENT$$', ocr_str)
            v_shop_image = v_shop_image + '<br>' + v_image

        v_all_image = v_all_image + '<br>' + v_shop_image
    v_html  = v_html.replace('$$IMAGE$$', v_all_image)

    print('Geneating {}.html,please wait...'.format(config['marketId']))
    with open('{}.html'.format(config['marketId']), 'w') as f:
        f.write(v_html)
    print('Geneating {}.html ok!'.format(config['marketId']))
    print('Elaspse Time: {}s'.format(get_seconds(start)))


'''
    功能：猫酷销售小票信息图片获取，生成html用于展示
'''

if __name__ == "__main__":
    config['mongo_db'] = get_ds_mongo_auth(config.get("mongo_string"))

    #打印配置信息
    print_dict(config)

    #生成朝合店铺小票html
    config['marketId'] = 'bjcyhsh'
    gen_html(config,shopMap_218)

    #生成上海店铺小票html
    config['marketId'] ='shhsh'
    gen_html(config,shopMap_110)


