#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/13 9:18
# @Author : 马飞
# @File : report_stats.py.py
# @Software: PyCharm
import sys
import datetime
import pymysql
import json
import traceback

'''
  全局配置
'''

today=datetime.date.today().strftime("%Y-%m-%d")
day7_ago=(datetime.date.today() + datetime.timedelta(days = -7)).strftime("%Y-%m-%d")
print('统计日期范围 : {}~{}'.format(day7_ago,today))

config = {
    'db_string': '10.2.39.17:23306:puppet:puppet:Puppet@123',
    'shape': 'line',
    'shape_disk_usage': 'bar',
    'start_date': day7_ago,
    'end_date': today,
    'cpu_title': 'cpu使用率[%]',
    'mem_title': '内存使用率[%]',
    'disk_usage_title': '磁盘使用率[%]',
    'disk_read_title': '磁盘读[KB/S]',
    'disk_write_title': '磁盘写[KB/S]',
    'net_in_title': '网络流入[KB/S]',
    'net_out_title': '网络流出[KB/S]',
    'db_total_title': '总连接数',
    'db_active_title': '活跃连接数',
    'db_bak_sz_title': '备份大小[MB]',
    'db_bak_tm_title': '备份时间[S]',
    'db_qps_title': '每秒查询率(QPS)',
    'db_tps_title': '每秒事务数(TPS)',
    'db_tbs_title': '表空间使用率[%]',
    'cpu_color': '#8CD5C2',
    'mem_color': '#95CACA',
    'disk_usage_color': '#5CADAD,#9CD5C4,#BCD5C5',
    'disk_read_color': '#8CD5C2',
    'disk_write_color': '#8CD5C2',
    'net_in_color': '#8CD5C2',
    'net_out_color': '#8CD5C5',
    'db_total_color': '#8CD5C2',
    'db_active_color': '#8CD5C5',
    'db_bak_sz_color': '#8CD5C5',
    'db_bak_tm_color': '#8CD5C5',
    'db_qps_color': '#8CD5C5',
    'db_tps_color': '#8CD5C5',
    'db_tbs_color': '#5CADAD,#9CD5C4,#BCD5C5,#6CADAD,#7CD5C4,#CCD5C5'
}

'''
  功能：参与统计的服务器和数据库列表
'''
tj_server =[
     # 商管-BI系统web服务器1
     {
        'server_id': 82,
        'db_id': '',
     },
     # 商管-BI系统web服务器2
     {
        'server_id': 83,
        'db_id': '',
     },
     # 商管-乐软系统web服务器
     {
       'server_id':84,
       'db_id'    :'',
     },
     # 商管-OA系统web服务器1
     {
        'server_id': 85,
        'db_id': '',
     },
     # 商管-OA系统web服务器2
     {
        'server_id': 86,
        'db_id': '',
     },
    # 商管-OA系统sso服务器1
    {
        'server_id': 87,
        'db_id': '',
    },
    # 商管-OA系统sso服务器2
    {
        'server_id': 88,
        'db_id': '',
    },
    # 商管-投资系统git服务器
    {
        'server_id': 89,
        'db_id': '',
    },
    # 商管-OA系统nginx服务器
    {
        'server_id': 90,
        'db_id': '',
    },
    # 商管-结算系统应用服务器1
    {
        'server_id': 95,
        'db_id': '',
    },
    # 商管-结算系统应用服务器2
    {
        'server_id': 96,
        'db_id': '',
    },
    # 商管-资产租赁系统-应用1
    {
        'server_id': 113,
        'db_id': '',
    },
]

'''
   功能：获取数据库连接
'''


def get_cfg(cfg):
    db_ip = cfg['db_string'].split(':')[0]
    db_port = cfg['db_string'].split(':')[1]
    db_service = cfg['db_string'].split(':')[2]
    db_user = cfg['db_string'].split(':')[3]
    db_pass = cfg['db_string'].split(':')[4]
    cfg['db_mysql_dict'] = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
    return cfg


def get_bzrq():
    return datetime.datetime.now().strftime("%Y年%m月%d日 %H时%M分%S秒")


'''
 功能：获取服务器信息
'''


def get_server(config):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute('select * from t_server where id={}'.format(config['server_id']))
    rs = cr.fetchone()
    return rs


'''
 功能：获取数据库信息
'''


def get_db_server(config):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute('select * from t_db_source where id={}'.format(config['db_id']))
    rs = cr.fetchone()
    return rs


'''
 功能:HTML模板
'''
html_templete = '''
<!DOCTYPE html>
<html>
    <head>
         <meta charset="utf-8">
         <title>商管项目运行周报</title>
         <link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/3.4.1/css/bootstrap.min.css" rel="stylesheet">
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
            ul {
              padding-bottom:30px;
            }
           .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
           .xwtable thead td {font-size: 18px;color: #333333;
                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                              border: 1px solid #ccc; font-weight:bold;}
           .xwtable thead th {font-size: 18px;color: #333333;
                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                              border: 1px solid #ccc; font-weight:bold;}
           .xwtable tbody tr {background: #fff;font-size: 18px;color: #666666;}
           .xwtable tbody tr.alt-row {background: #f2f7fc;}
           .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
           span { color:red;}

         </style>
    </head>	
    <body>
       <div class="jumbotron jumbotron-fluid">
            <div class="container">
                  <h1>$$HEADER$$-运行周报</h1>                
                  <p>编制日期：$$BZRQ$$</p>
                  <p>编制人员：好生活平台组</p>
                  <p>统计日期：$$TJRQQ$$至$$TJRQZ$$</p>
            </div>
       </div>


       <!--运行情况 -->
       <div class="panel panel-default">
         <div class="panel-body">
                <ul class="nav nav-pills m-b-30 pull-left">
                    <li class="active">
                        <a href="#sg-week" data-toggle="tab" aria-expanded="true">本周运行情况</a>
                    </li>
                    <li class="">
                        <a href="#sg-server" data-toggle="tab" aria-expanded="true">服务器运行情况</a>
                    </li>
                    <li  class="">
                        <a href="#sg-db" data-toggle="tab" aria-expanded="false">$$DB_TAB$$</a>
                    </li>
                </ul>

                <div class="tab-content br-n pn">
                    <div id="sg-week" class="tab-pane active">
                         <div class="row">
                                <div class="col-md-12">
                                    <h3>$$SERVER_TITLE$$</h3>
                                    <div id='server_info' style="height:200px;">
                                       $$SERVER_TABLE$$
                                    </div>
                                 </div>
                           </div>
                           <div class="row">
                                 <div class="col-md-12">
                                    <h3>$$DB_TITLE$$</h3>
                                    <div id='db_info' style="height:200px;">
                                       $$DB_TABLE$$
                                    </div>
                                 </div>
                           </div> 
                    </div>
                    <div id="sg-server" class="tab-pane">
                          <div class="row">
                                <div class="col-md-12">
                                    <div id='cpu_usage' style="height:400px;"></div>
                                 </div>
                           </div>
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='mem_usage' style="height:400px;"></div>
                                 </div>
                           </div> 
                            <div class="row">
                                 <div class="col-md-12">
                                    <div id='disk_usage' style="height:400px;"></div>
                                 </div>
                           </div>                           
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='disk_read' style="height:400px;"></div>
                                 </div>
                           </div> 
                            <div class="row">
                                 <div class="col-md-12">
                                    <div id='disk_write' style="height:400px;"></div>
                                 </div>
                           </div>                           
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='net_in' style="height:400px;"></div>
                                 </div>
                           </div> 
                            <div class="row">
                                 <div class="col-md-12">
                                    <div id='net_out' style="height:400px;"></div>
                                 </div>
                           </div>
                    </div>
                    <div id="sg-db" class="tab-pane">
                           <div class="row">
                                <div class="col-md-12">
                                    <div id='db_total_num' style="height:400px;"></div>
                                 </div>
                           </div>
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='db_active_num' style="height:400px;"></div>
                                 </div>
                           </div> 
                           <div class="row">
                                <div class="col-md-12">
                                    <div id='db_bak_sz' style="height:400px;"></div>
                                 </div>
                           </div>
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='db_bak_tm' style="height:400px;"></div>
                                 </div>
                           </div>     
                           <div class="row">
                                <div class="col-md-12">
                                    <div id='db_qps_num' style="height:400px;"></div>
                                 </div>
                           </div>
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='db_tps_num' style="height:400px;"></div>
                                 </div>
                           </div>   
                           <div class="row">
                                 <div class="col-md-12">
                                    <div id='db_tbs_usage' style="height:400px;"></div>
                                 </div>
                           </div>
                    </div>
                </div>
          </div>
       </div>

       <script src="https://cdn.bootcdn.net/ajax/libs/jquery/3.5.1/jquery.js"></script>
       <script src="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/3.4.1/js/bootstrap.min.js"></script>
       <script src="https://cdn.bootcdn.net/ajax/libs/echarts/4.8.0/echarts.min.js"></script>
       <script>
          function gen_charts(p_id,p_option) {
              var myChart = echarts.init($(p_id)[0]);
              var option  = p_option
              myChart.setOption(option);
          }

          $(document).ready(function() {

            $('a[data-toggle="tab"]').on('show.bs.tab', function (e) {
                  var activeTab = $(e.target).text();
                  var previousTab = $(e.relatedTarget).text();
                  console.log(activeTab,previousTab)

                  if (activeTab=='本周运行情况') {
                     $('#sg-week').show();
                     $('#sg-server').hide();
                     $('#sg-db').hide();
                  }

                  if (activeTab=='服务器运行情况') {
                     $('#sg-week').hide();
                     $('#sg-server').show();
                     $('#sg-db').hide();
                     gen_charts('#cpu_usage',  $cpu_option);
                     gen_charts('#mem_usage',  $mem_option);
                     gen_charts('#disk_usage', $disk_usage_option);
                     gen_charts('#disk_read',  $disk_read_option);
                     gen_charts('#disk_write', $disk_write_option);
                     gen_charts('#net_in',     $net_in_option);
                     gen_charts('#net_out',    $net_out_option);
                  }

                  if (activeTab=='数据库运行情况') {
                     $('#sg-week').hide();
                     $('#sg-server').hide();
                     $('#sg-db').show();
                     gen_charts('#db_total_num',  $db_total_option);
                     gen_charts('#db_active_num', $db_active_option);
                     gen_charts('#db_bak_sz',     $db_bak_sz_option);
                     gen_charts('#db_bak_tm',     $db_bak_tm_option);
                     gen_charts('#db_qps_num',    $db_qps_option);
                     gen_charts('#db_tps_num',    $db_tps_option);
                     gen_charts('#db_tbs_usage',  $db_tbs_option);
                  }
             });

          })
        </script>
    </body>
</html>
'''

'''
  功能：服务器概述
'''
st_server = '''
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
           'cpu使用率'                      AS flag, 
           ROUND(MIN(cpu_total_usage),2)   AS val_min,
	       ROUND(MAX(cpu_total_usage),2)   AS val_max,
	       ROUND(AVG(cpu_total_usage),2)   AS val_avg,
	      '阀值:连续3次>85%'                AS threshold,
	      '未触发告警'                      AS message
      FROM `t_monitor_task_server_log`
      WHERE server_id=$$SERVER_ID$$
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          cpu_total_usage   AS val_cur
      FROM `t_monitor_task_server_log` 
      WHERE server_id=$$SERVER_ID$$ 
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
           '内存使用率'                   AS flag, 
           ROUND(MIN(mem_usage),2)       AS val_min,
	       ROUND(MAX(mem_usage),2)       AS val_max,
	       ROUND(AVG(mem_usage),2)       AS val_avg,
	      '阀值:连续3次>85%'              AS threshold,
	      '未触发告警'                    AS message
      FROM `t_monitor_task_server_log`
      WHERE server_id=$$SERVER_ID$$
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          mem_usage   AS val_cur
      FROM `t_monitor_task_server_log` 
      WHERE server_id=$$SERVER_ID$$ 
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
           '磁盘使用率'                     AS flag, 
            MIN(disk_usage)               AS val_min,
            MAX(disk_usage)               AS val_max,
	        ''                            AS val_avg,
	       '阀值:连续3次>80%'              AS threshold,
	       '未触发告警'                    AS message
      FROM `t_monitor_task_server_log`
      WHERE server_id=$$SERVER_ID$$
       and  disk_usage is not null and disk_usage!='' and disk_usage!='None'
       and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59' ) a,
     (SELECT 
          disk_usage   AS val_cur
      FROM `t_monitor_task_server_log` 
      WHERE server_id=$$SERVER_ID$$ 
       and  disk_usage is not null and disk_usage!='' and disk_usage!='None'
       and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
'''

st_db = '''
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
           '总连接数' AS flag,  
	       MIN(total_connect+0) AS val_min,
	       MAX(total_connect+0) AS val_max,
	       ROUND(AVG(total_connect+0),2) AS val_avg,
	      '阀值 >300'                   AS threshold,
	      '未触发告警'                   AS message
      FROM `t_monitor_task_db_log`
      WHERE db_id=$$DB_ID$$
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          total_connect+0  AS val_cur
      FROM `t_monitor_task_db_log` 
      WHERE db_id=$$DB_ID$$ AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
       '活跃连接数' AS flag,  
	   MIN(active_connect+0) AS val_min,
	   MAX(active_connect+0) AS val_max,
	   ROUND(AVG(total_connect+0),2) AS val_avg,
	   '阀值 >100'                   AS threshold,
	   '未触发告警'                   AS message
      FROM `t_monitor_task_db_log`
      WHERE db_id=$$DB_ID$$
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          active_connect+0  AS val_cur
      FROM `t_monitor_task_db_log` 
      WHERE db_id=$$DB_ID$$ 
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
       '每秒查询率(QPS)'       AS flag,  
	   MIN(db_qps+0)          AS val_min,
	   MAX(db_qps+0)          AS val_max,
	   ROUND(AVG(db_qps+0),2) AS val_avg,
	   '阀值 >600'             AS threshold,
	   '未触发告警'             AS message
      FROM `t_monitor_task_db_log`
      WHERE db_id=$$DB_ID$$
       and  db_qps is not null and db_qps!='' 
       and  create_date BETWEEN '$$TJRQQ$$' AND '$$TJRQZ$$') a,
     (SELECT 
          db_qps+0  AS val_cur
      FROM `t_monitor_task_db_log` 
      WHERE db_id=$$DB_ID$$ 
       and  db_qps is not null and db_qps!='' 
       and  create_date BETWEEN '$$TJRQQ$$' AND '$$TJRQZ$$' 
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
       '每秒事务数(TPS)'        AS flag,  
	    MIN(db_tps+0)          AS val_min,
	    MAX(db_tps+0)          AS val_max,
	    ROUND(AVG(db_tps+0),2) AS val_avg,
	   '阀值 >100'             AS threshold,
	   '未触发告警'             AS message
      FROM `t_monitor_task_db_log`
      WHERE db_id=$$DB_ID$$
       and  db_tps is not null and db_tps!='' 
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          db_tps+0  AS val_cur
      FROM `t_monitor_task_db_log` 
      WHERE db_id=$$DB_ID$$ 
      and  db_tps is not null and db_tps!='' 
      and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
       '表空间使用率'            AS flag,  
	    IFNULL(MIN(db_tbs_usage),'-') AS val_min,
        IFNULL(MAX(db_tbs_usage),'-') AS val_max,
        '' AS val_avg,
        CASE WHEN MIN(db_tbs_usage) IS NULL THEN '-' ELSE '阀值 >85%'  END   AS threshold,
        CASE WHEN MIN(db_tbs_usage) IS NULL THEN '-' ELSE '未触发告警' END   AS message
      FROM `t_monitor_task_db_log`
      WHERE db_id=$$DB_ID$$
       and  db_tbs_usage is not null and db_tbs_usage!='' and db_tbs_usage!='None'
       AND  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59') a,
     (SELECT 
          db_tbs_usage  AS val_cur
      FROM `t_monitor_task_db_log` 
      WHERE db_id=$$DB_ID$$ 
      and  db_tbs_usage is not null and db_tbs_usage!='' and db_tbs_usage!='None'
      and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
            '备份大小[MB]' AS flag,
            MIN(CASE 
                WHEN INSTR(total_size,'G')>0 THEN REPLACE(total_size,'G','')*1024
                WHEN INSTR(total_size,'M')>0 THEN REPLACE(total_size,'M','')
                ELSE REPLACE(total_size,'K','')/1024
            END) AS val_min,
            MAX(CASE 
                WHEN INSTR(total_size,'G')>0 THEN REPLACE(total_size,'G','')*1024
                WHEN INSTR(total_size,'M')>0 THEN REPLACE(total_size,'M','')
                ELSE REPLACE(total_size,'K','')/1024
            END) AS val_max,
            ROUND(AVG(
            CASE 
                WHEN INSTR(total_size,'G')>0 THEN REPLACE(total_size,'G','')*1024
                WHEN INSTR(total_size,'M')>0 THEN REPLACE(total_size,'M','')
                ELSE REPLACE(total_size,'K','')/1024
            END),2) AS val_avg,
            ''    AS threshold,
            ''    AS message
        FROM `t_db_backup_total`
        WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id=$$DB_ID$$) 
         AND  create_date between '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59' ) a,
     (SELECT 
          CASE 
                WHEN INSTR(total_size,'G')>0 THEN REPLACE(total_size,'G','')*1024
                WHEN INSTR(total_size,'M')>0 THEN REPLACE(total_size,'M','')
                ELSE REPLACE(total_size,'K','')/1024
          END  AS val_cur
      FROM `t_db_backup_total` 
      WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id=$$DB_ID$$) 
       and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	    ORDER BY create_date DESC LIMIT 1) b
UNION ALL
SELECT     
   a.flag,  
   a.val_min,
   a.val_max,
   a.val_avg,
   b.val_cur,
   a.threshold,
   a.message
FROM (SELECT 
            '备份时长[S]' AS flag,
            MIN(elaspsed_backup) AS val_min,
            MAX(elaspsed_backup) AS val_max,
            ROUND(AVG(elaspsed_backup),2) AS val_avg,
            ''    AS threshold,
            ''    AS message
        FROM `t_db_backup_total`
        WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id=$$DB_ID$$) 
         AND  create_date between '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59' ) a,
     (SELECT 
         elaspsed_backup AS val_cur
      FROM `t_db_backup_total` 
      WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id=$$DB_ID$$) 
       and  create_date BETWEEN '$$TJRQQ$$ 0:0:0' and '$$TJRQZ$$ 23:59:59'
	    ORDER BY create_date DESC LIMIT 1) b
'''

'''
  功能：服务器指标统计语句
'''
st_cpu = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H') AS rq,
                           ROUND(AVG(cpu_total_usage),2) AS val 
                   FROM `t_monitor_task_server_log` 
                   WHERE server_id={}  
                     AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H') 
                   order by 1'''

st_mem = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') AS rq,
                           ROUND(AVG(mem_usage),2)  AS val 
                   FROM `t_monitor_task_server_log` 
                   WHERE server_id={}  
                      AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') 
                   order by 1'''

st_disk_usage = '''SELECT DATE_FORMAT(create_date, '%Y-%m-%d') AS rq, 
                          disk_usage AS val 
                   FROM  `t_monitor_task_server_log` 
                   WHERE server_id = {} 
                      AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   ORDER BY 1'''

st_disk_read = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') AS rq,
                           ROUND(AVG(disk_read/1024),2)  AS val 
                   FROM `t_monitor_task_server_log` 
                   WHERE server_id={}  
                     AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') 
                   order by 1'''

st_disk_write = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') AS rq,
                           ROUND(AVG(disk_write/1024),2)  AS val 
                   FROM `t_monitor_task_server_log` 
                   WHERE server_id={}  
                     AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') 
                    order by 1'''

st_net_in = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') AS rq,
                           ROUND(AVG(net_in/1024),2)  AS val 
                 FROM `t_monitor_task_server_log` 
                 WHERE server_id={}  
                     AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                   GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') 
                   order by 1'''

st_net_out = '''SELECT  DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') AS rq,
                        ROUND(AVG(net_out/1024),2)  AS val 
                FROM `t_monitor_task_server_log` 
                 WHERE server_id={}  
                 AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %H:%i') 
                order by 1'''

'''
  功能：数据库指标统计语句
'''
st_db_total_num = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') AS rq,
                        ROUND(AVG(total_connect),2) AS val
                     FROM `t_monitor_task_db_log`
                       WHERE server_id={}
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                       GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') 
                     order by 1'''

st_db_active_num = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') AS rq,
                        ROUND(AVG(active_connect),2) AS val
                      FROM `t_monitor_task_db_log`
                       WHERE server_id={}
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                       GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') 
                      order by 1'''

st_db_qps_num = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') AS rq,
                        ROUND(AVG(db_qps),2) AS val
                     FROM `t_monitor_task_db_log`
                       WHERE server_id={}
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                         and  db_qps is not null and db_qps!='' 
                       GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') 
                     order by 1'''

st_db_tps_num = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') AS rq,
                        ROUND(AVG(db_tps),2) AS val
                      FROM `t_monitor_task_db_log`
                       WHERE server_id={}
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                         and  db_tps is not null and db_tps!='' 
                       GROUP BY DATE_FORMAT(create_date,'%Y-%m-%d %h:%i') 
                      order by 1'''

st_db_bak_size = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d') AS rq,
                        CASE 
                            WHEN INSTR(total_size,'G')>0 THEN REPLACE(total_size,'G','')*1024
                            WHEN INSTR(total_size,'M')>0 THEN REPLACE(total_size,'M','')
                            ELSE REPLACE(total_size,'K','')/1024
                        END AS val
                    FROM `t_db_backup_total`
                       WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id={}) 
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                   order by 1'''

st_db_bak_time = '''SELECT 
                        DATE_FORMAT(create_date,'%Y-%m-%d') AS rq,
                        elaspsed_backup AS val
                    FROM `t_db_backup_total`
                       WHERE db_tag=(SELECT db_tag FROM t_db_config WHERE db_id={}) 
                         AND  create_date BETWEEN '{} 0:0:0' AND '{} 23:59:59' 
                    order by 1'''

st_db_tbs_usage = '''SELECT DATE_FORMAT(create_date, '%Y-%m-%d') AS rq, 
                          db_tbs_usage AS val 
                    FROM  `t_monitor_task_db_log` 
                    WHERE db_id = {} 
                      AND  create_date between '{} 0:0:0' and '{} 23:59:59' 
                      and  db_tbs_usage is not null and db_tbs_usage!='' and db_tbs_usage!='None'
                   ORDER BY 1'''

'''  
   功能：获取mysql连接，以字典返回
'''


def get_ds_mysql_dict(ip, port, service, user, password):
    conn = pymysql.connect(host=ip, port=int(port),
                           user=user, passwd=password,
                           db=service, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn


'''
  功能：打印字典
'''


def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', config[key])
    print('-'.ljust(85, '-'))


'''
  功能：根据st语句返回结果集cpu,mem
'''


def get_result(config, st):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    x = []
    y = []
    for i in rs:
        x.append(i['rq'])
        y.append(i['val'])
    return str(x), str(y)


'''
  功能：根据st语句返回结果集db_sz,db_tm
'''


def get_result_pie(config, st):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    v = ''
    js = json.dumps(rs)


'''
  功能：根据最大磁盘使用率
'''


def get_max_disk_usage(v_max):
    try:
        d_max = json.loads(v_max)
        n_val = 0.0
        for key in d_max:
            if n_val <= float(d_max[key]):
                n_val = float(d_max[key])
        print(n_val)
        return n_val
    except:
        return 0


'''
  功能：格式化字典
'''


def format_dict(p_dict):
    try:
        v = ''
        d = json.loads(p_dict)
        for k in d:
            v = v + '{}:{}<br>'.format(k, str(d[k]) + '%')
        return v[0:-4]
    except:
        print(traceback.print_exc())
        return ''


'''
  功能：根据st语句返回结果集server_info
'''


def get_result_server(config, st):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    msg = '<table class="xwtable"><thead><tr><td>指标</td><td>最小值</td><td>最大值</td><td>平均值</td><td>当前值</td><td>告值阀值</td><td>备注</td></tr></thead>'
    for r in rs:
        if r['flag'] in ('磁盘使用率', '表空间使用率') and get_max_disk_usage(r['val_cur']) < 80:
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'],
                       format_dict(r['val_min']),
                       format_dict(r['val_max']),
                       r['val_avg'],
                       format_dict(r['val_cur']),
                       r['threshold'],
                       '未触发告警')

        elif r['flag'] in ('磁盘使用率', '表空间使用率') and get_max_disk_usage(r['val_cur']) >= 80:
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'],
                       format_dict(r['val_min']),
                       format_dict(r['val_max']),
                       r['val_avg'],
                       format_dict(r['val_cur']),
                       r['threshold'],
                       '<span style="color:red">已触发告警</span>')
        else:
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'], r['val_min'] + '%', r['val_max'] + '%', r['val_avg'] + '%', r['val_cur'] + '%',
                       r['threshold'], r['message'])

    return msg + '</tbody></table>'


'''
  功能：根据st语句返回结果集db_info
'''


def get_result_db(config, st):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    msg = '<table class="xwtable"><thead><tr><td>指标</td><td>最小值</td><td>最大值</td><td>平均值</td><td>当前值</td><td>告值阀值</td><td>备注</td></tr></thead>'
    for r in rs:
        if r['flag'] in ('总连接数'):
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'],
                       r['val_min'],
                       r['val_max'],
                       r['val_avg'],
                       r['val_cur'],
                       r['threshold'],
                       '未触发告警' if int(r['val_cur']) < 300 else '<span style="color:red">已触发告警</span>'
                       )

        elif r['flag'] in ('活跃连接数'):
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'],
                       r['val_min'],
                       r['val_max'],
                       r['val_avg'],
                       r['val_cur'],
                       r['threshold'],
                       '未触发告警' if int(r['val_cur']) < 100 else '<span style="color:red">已触发告警</span>'
                       )
        elif r['flag'] in ('表空间使用率'):
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'],
                       format_dict(r['val_min']),
                       format_dict(r['val_max']),
                       r['val_avg'],
                       format_dict(r['val_cur']),
                       r['threshold'],
                       '未触发告警' if get_max_disk_usage(r['val_cur']) < 85 else '<span style="color:red">已触发告警</span>'
                       )
        else:
            msg = msg + '<tbody><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'. \
                format(r['flag'], r['val_min'], r['val_max'], r['val_avg'], r['val_cur'], r['threshold'], r['message'])

    return msg + '</tbody></table>'


'''
  功能：根据st语句返回结果集disk_usage
'''


def get_disk_usage_result(config, st):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    disk = {}
    for d in rs:
        disk[d['rq']] = json.loads(d['val'])
    x = []
    y = []
    for d in disk:
        x.append(d)
        y.append(disk[d])
    return str(x), y


'''
  功能：生成echarts画图options
'''


def gen_options(p_title, p_legend, x_data, y_series):
    v = '''{
                title: {
                    text: 'p_title'
                },
                tooltip: { 
                    trigger: 'axis',
                    axisPointer: {
                        type: 'cross',
                        label: {
                            backgroundColor: '#6a7985'
                        }
                      }
                },
                toolbox: {
                　　show: true,
                　　feature: {
                　　　　saveAsImage: {
                　　　　show:true,
                　　　　excludeComponents :['toolbox'],
                　　　　pixelRatio: 2
                　　　　}
                　　}
                },
                legend: {
                    data:['p_legend']
                },
                xAxis: {
                    data: x_data
                },
                yAxis: {
                    type: 'value',
                    max: 100, 
                    min: 0, 
                },
                series: y_series
        }'''
    v = v.replace('p_title', p_title)
    v = v.replace('p_legend', p_legend)
    v = v.replace('x_data', x_data)
    v = v.replace('y_series', y_series)
    return v


'''
  功能：生成echarts画图options
'''


def gen_options_io_net(p_title, p_legend, x_data, y_series):
    v = '''{
                title: {
                    text: 'p_title'
                },
                tooltip: { 
                    trigger: 'axis',
                    axisPointer: {
                        type: 'cross',
                        label: {
                            backgroundColor: '#6a7985'
                        }
                      }
                },
                toolbox: {
                　　show: true,
                　　feature: {
                　　　　saveAsImage: {
                　　　　show:true,
                　　　　excludeComponents :['toolbox'],
                　　　　pixelRatio: 2
                　　　　}
                　　}
                },
                legend: {
                    data:['p_legend']
                },
                xAxis: {
                    data: x_data
                },
                yAxis: {
                    type: 'value'
                },
                series: y_series
        }'''
    v = v.replace('p_title', p_title)
    v = v.replace('p_legend', p_legend)
    v = v.replace('x_data', x_data)
    v = v.replace('y_series', y_series)
    return v


'''
  功能：生成echarts画图series(cpu,mem)
'''


def gen_series(p_title, y_data, p_color):
    t = []
    y = '''{ name: 'p_title', type: 'p_line',areaStyle:{normal:{color:'p_color'}},itemStyle:{normal:{color:'p_color',lineStyle:{color:'p_color'}}},data: y_data}'''
    y = y.replace('p_title', p_title).replace('y_data', y_data).replace('p_line', config['shape']).replace('p_color',
                                                                                                           p_color)
    t.append(y)
    return str(t).replace('"', '')


'''
  功能：生成echarts画图series(db_sz,db_tm)
'''


def gen_series_bar(p_title, y_data, p_color):
    t = []
    y = '''{ name: 'p_title', type: 'bar',areaStyle:{normal:{color:'p_color'}},itemStyle:{normal:{color:'p_color',lineStyle:{color:'p_color'}}},data: y_data}'''
    y = y.replace('p_title', p_title).replace('y_data', y_data).replace('p_line', config['shape']).replace('p_color',
                                                                                                           p_color)
    t.append(y)
    return str(t).replace('"', '')


'''
  功能：生成echarts画图series(disk_usage)
'''


def gen_series_disk_usage(y_data, p_color):
    # 获取图例列表
    h = []
    for data in y_data:
        for ckey in data:
            h.append(ckey)
        break

    # 处理传入字典为一个实单的列表
    t = []
    for data in y_data:
        v = ''
        for ckey in data:
            v = v + str(data[ckey]) + '@'
        t.append(v[0:-1])

    # 动态初始化字典元素为列表
    d = {}
    for v in t:
        for i in range(len(v.split('@'))):
            d[i] = []
        break

    # 写入字典
    for v in t:
        for i in range(len(v.split('@'))):
            d[i].append(float(v.split('@')[i]))

    # 生成series字符串
    s = []
    for i in range(len(h)):
        y = '''{ name: 'p_title', type: 'p_line',areaStyle:{normal:{color:'p_color'}},itemStyle:{normal:{color:'p_color',lineStyle:{color:'p_color'}}}, data: y_data}'''
        y = y.replace('p_title', h[i]).replace('y_data', str(d[i])).replace('p_line',
                                                                            config['shape_disk_usage']).replace(
            'p_color', p_color.split(',')[i])
        s.append(y)
    s = str(s).replace('"', '')
    return s, "','".join(h)


'''
 功能：生成html
'''


def gen_html(cfg, html_templete):
    # 更新模板
    html_templete = html_templete.replace('$$HEADER$$', get_server(cfg)['server_desc'])
    html_templete = html_templete.replace('$$TJRQQ$$', config['start_date'])
    html_templete = html_templete.replace('$$TJRQZ$$', config['end_date'])
    html_templete = html_templete.replace('$$BZRQ$$', get_bzrq())

    # cpu使用率
    x_cpu, y_cpu = get_result(config, st_cpu.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    cpu_series = gen_series(config['cpu_title'], y_cpu, config['cpu_color'])
    cpu_option = gen_options(config['cpu_title'], config['cpu_title'], x_cpu, cpu_series)

    # 内存使用率
    x_mem, y_mem = get_result(config, st_mem.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    mem_series = gen_series(config['mem_title'], y_mem, config['mem_color'])
    mem_option = gen_options(config['mem_title'], config['mem_title'], x_mem, mem_series)

    # 磁盘使用率
    x_disk_uage, y_disk_usage = get_disk_usage_result(config, st_disk_usage.format(cfg['server_id'], cfg['start_date'],
                                                                                   cfg['end_date']))
    disk_usage_series, disk_usage_title = gen_series_disk_usage(y_disk_usage, config['disk_usage_color'])
    disk_usage_option = gen_options(config['disk_usage_title'], disk_usage_title, x_disk_uage, disk_usage_series)

    # 磁盘读
    x_disk_read, y_disk_read = get_result(config,
                                          st_disk_read.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    disk_read_series = gen_series(config['disk_read_title'], y_disk_read, config['disk_read_color'])
    disk_read_option = gen_options_io_net(config['disk_read_title'], config['disk_read_title'], x_disk_read,
                                          disk_read_series)

    # 磁盘写
    x_disk_write, y_disk_write = get_result(config,
                                            st_disk_write.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    disk_write_series = gen_series(config['disk_write_title'], y_disk_write, config['disk_write_color'])
    disk_write_option = gen_options_io_net(config['disk_write_title'], config['disk_write_title'], x_disk_write,
                                           disk_write_series)

    # 网络流入
    x_net_in, y_net_in = get_result(config, st_net_in.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    net_in_series = gen_series(config['net_in_title'], y_net_in, config['net_in_color'])
    net_in_option = gen_options_io_net(config['net_in_title'], config['net_in_title'], x_net_in, net_in_series)

    # 网络流出
    x_net_out, y_net_out = get_result(config, st_net_out.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
    net_out_series = gen_series(config['net_out_title'], y_net_out, config['net_out_color'])
    net_out_option = gen_options_io_net(config['net_out_title'], config['net_out_title'], x_net_out, net_out_series)

    # 更新服务器图表
    html_templete = html_templete.replace('$cpu_option', cpu_option)
    html_templete = html_templete.replace('$mem_option', mem_option)
    html_templete = html_templete.replace('$disk_usage_option', disk_usage_option)
    html_templete = html_templete.replace('$disk_read_option', disk_read_option)
    html_templete = html_templete.replace('$disk_write_option', disk_write_option)
    html_templete = html_templete.replace('$net_in_option', net_in_option)
    html_templete = html_templete.replace('$net_out_option', net_out_option)

    # 更新服务器信息
    html_templete = html_templete.replace('$$SERVER_TITLE$$', '服务器地址:{}'.format(get_server(cfg)['server_ip']))
    html_templete = html_templete.replace('$$SERVER_TABLE$$',
                                          get_result_server(config,
                                                            st_server.replace('$$SERVER_ID$$', str(config['server_id']))
                                                            .replace('$$TJRQQ', config['start_date'])
                                                            .replace('$$TJRQZ', config['end_date'])
                                                            ))

    if cfg['db_id'] != '':
        # 数据库总连接
        x_db_total, y_db_total = get_result(config,
                                            st_db_total_num.format(str(cfg['server_id']), cfg['start_date'],
                                                                   cfg['end_date']))
        db_total_series = gen_series(config['db_total_title'], y_db_total, config['db_total_color'])
        db_total_option = gen_options_io_net(config['db_total_title'], config['db_total_title'], x_db_total,
                                             db_total_series)

        # 数据库活跃连接
        x_db_active, y_db_active = get_result(config,
                                              st_db_active_num.format(cfg['server_id'], cfg['start_date'],
                                                                      cfg['end_date']))
        db_active_series = gen_series(config['db_active_title'], y_db_active, config['db_active_color'])
        db_active_option = gen_options_io_net(config['db_active_title'], config['db_active_title'], x_db_active,
                                              db_active_series)

        # 数据库QPS
        x_db_qps, y_db_qps = get_result(config,
                                        st_db_qps_num.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
        db_qps_series = gen_series(config['db_qps_title'], y_db_qps, config['db_qps_color'])
        db_qps_option = gen_options_io_net(config['db_qps_title'], config['db_qps_title'], x_db_qps, db_qps_series)

        # 数据库TPS
        x_db_tps, y_db_tps = get_result(config,
                                        st_db_tps_num.format(cfg['server_id'], cfg['start_date'], cfg['end_date']))
        db_tps_series = gen_series(config['db_tps_title'], y_db_tps, config['db_tps_color'])
        db_tps_option = gen_options_io_net(config['db_tps_title'], config['db_tps_title'], x_db_tps, db_tps_series)

        # 数据库备份大小
        x_db_bak_sz, y_db_bak_sz = get_result(config,
                                              st_db_bak_size.format(cfg['db_id'], cfg['start_date'], cfg['end_date']))
        db_bak_sz_series = gen_series_bar(config['db_bak_sz_title'], y_db_bak_sz, config['db_bak_sz_color'])
        db_bak_sz_option = gen_options_io_net(config['db_bak_sz_title'], config['db_bak_sz_title'], x_db_bak_sz,
                                              db_bak_sz_series)

        # 数据库备份时长
        x_db_bak_tm, y_db_bak_tm = get_result(config,
                                              st_db_bak_time.format(cfg['db_id'], cfg['start_date'], cfg['end_date']))
        db_bak_tm_series = gen_series_bar(config['db_bak_tm_title'], y_db_bak_tm, config['db_bak_tm_color'])
        db_bak_tm_option = gen_options_io_net(config['db_bak_tm_title'], config['db_bak_tm_title'], x_db_bak_tm,
                                              db_bak_tm_series)

        # 数据库表空间使用率
        x_db_tbs_usage, y_db_tbs_usage = get_disk_usage_result(config,
                                                               st_db_tbs_usage.format(cfg['db_id'], cfg['start_date'],
                                                                                      cfg['end_date']))

        if x_db_tbs_usage != '[]':
            db_tbs_series, db_tbs_title = gen_series_disk_usage(y_db_tbs_usage, config['db_tbs_color'])
            db_tbs_option = gen_options(config['db_tbs_title'], db_tbs_title, x_db_tbs_usage, db_tbs_series)
            html_templete = html_templete.replace('$db_tbs_option', db_tbs_option)

        # 更新数据库图表
        html_templete = html_templete.replace('$db_total_option', db_total_option)
        html_templete = html_templete.replace('$db_active_option', db_active_option)
        html_templete = html_templete.replace('$db_qps_option', db_qps_option)
        html_templete = html_templete.replace('$db_tps_option', db_tps_option)
        html_templete = html_templete.replace('$db_bak_sz_option', db_bak_sz_option)
        html_templete = html_templete.replace('$db_bak_tm_option', db_bak_tm_option)
        html_templete = html_templete.replace('$$DB_TITLE$$', '数据库地址:{}/{}'.
                                              format(get_db_server(cfg)['ip'], get_db_server(cfg)['port']))
        html_templete = html_templete.replace('$$DB_TAB$$', '数据库运行情况')
        html_templete = html_templete.replace('$$DB_TABLE$$',
                                              get_result_db(config,
                                                            st_db.replace('$$DB_ID$$', str(config['db_id']))
                                                            .replace('$$TJRQQ', config['start_date'])
                                                            .replace('$$TJRQZ', config['end_date'])))

    else:
        html_templete = html_templete.replace('$db_total_option', "''")
        html_templete = html_templete.replace('$db_active_option', "''")
        html_templete = html_templete.replace('$db_qps_option', "''")
        html_templete = html_templete.replace('$db_tps_option', "''")
        html_templete = html_templete.replace('$db_bak_sz_option', "''")
        html_templete = html_templete.replace('$db_bak_tm_option', "''")
        html_templete = html_templete.replace('$db_tbs_option', "''")
        html_templete = html_templete.replace('$$DB_TITLE$$', '&nbsp;')
        html_templete = html_templete.replace('$$DB_TABLE$$', '&nbsp;')
        html_templete = html_templete.replace('$$DB_TAB$$', '&nbsp;')
    return html_templete


def write_html(cfg):
    if cfg['db_id'] != '':
        filename = '{}-运行周报_db_{}.html'.format(get_server(cfg)['server_desc'], cfg['server_id'])
    else:
        filename = '{}-运行周报_server_{}.html'.format(get_server(cfg)['server_desc'], cfg['db_id'])
    print('write html: {} ok'.format(filename))
    with open(filename, 'w') as f:
        v = gen_html(cfg, html_templete)
        f.write(v)


def stats():
    cfg = get_cfg(config)
    for s in tj_server:
        print(s['server_id'], s['db_id'])
        cfg['server_id'] = s['server_id']
        cfg['db_id'] = s['db_id']
        write_html(cfg)


if __name__ == "__main__":
    stats()


