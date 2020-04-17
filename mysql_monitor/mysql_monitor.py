#!/home/hopson/apps/usr/webserver/python3.6.0/bin/python3
# -*- coding:utf-8 -*-
import sys
import traceback
import os
import configparser
import time
import datetime
import smtplib
import pymysql
import xlwt
import warnings
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.utils import formatdate

def get_conn(p_ip,p_port,p_service,p_user,p_pass):   
    conn = pymysql.connect(host=p_ip, port=int(p_port), user=p_user, passwd=p_pass, db=p_service,use_unicode=True, charset="utf8")
    return conn

def exception_info():
    e_str=traceback.format_exc()
    #print('e_str:',e_str)
    while True:
      if e_str[-1]=='\n' or e_str[-1]=='\r' :
        e_str=e_str[0:-1]
        continue
      else:
        break
    return e_str[e_str.find("pymysql.err."):]

def format_error(msg):
    p_msg=msg[0:msg.find('During')].\
                  replace("pymysql.err.InternalError: (",""). \
                  replace("pymysql.err.ProgrammingError: (", ""). \
                  replace("pymysql.err.OperationalError: (",""). \
                  replace(")","").replace('"','').replace("'","").split(',')
    return p_msg

def get_mon_conf_transfer(fname):
    config=configparser.ConfigParser()
    config.read(fname,encoding="utf-8-sig")
    transfer_server_sour   = config.get("TRANSFER","transfer_server_sour")
    transfer_server_dest   = config.get("TRANSFER","transfer_server_dest")    
    transfer_type          = config.get("TRANSFER", "transfer_type")
    transfer_table         = config.get("TRANSFER", "transfer_table")
    transfer_sour_db       = config.get("TRANSFER","transfer_sour_db")
    transfer_dest_db       = config.get("TRANSFER","transfer_dest_db") 
    transfer_table         = config.get("TRANSFER","transfer_table")
    transfer_tab_part_range= config.get("TRANSFER","transfer_tab_part_range")
    db_sour_ip             = transfer_server_sour.split(':')[0]
    db_sour_port           = transfer_server_sour.split(':')[1]
    db_sour_service        = transfer_server_sour.split(':')[2]
    db_sour_user           = transfer_server_sour.split(':')[3]
    db_sour_pass           = transfer_server_sour.split(':')[4]
    db_dest_ip             = transfer_server_dest.split(':')[0]
    db_dest_port           = transfer_server_dest.split(':')[1]
    db_dest_service        = transfer_server_dest.split(':')[2]
    db_dest_user           = transfer_server_dest.split(':')[3]
    db_dest_pass           = transfer_server_dest.split(':')[4]
    config = {}
    config['db_sour_ip']       = db_sour_ip
    config['db_sour_port']     = db_sour_port
    config['db_sour_service']  = db_sour_service
    config['db_sour_user']     = db_sour_user
    config['db_sour_pass']     = db_sour_pass
    config['db_dest_ip']       = db_dest_ip
    config['db_dest_port']     = db_dest_port
    config['db_dest_service']  = db_dest_service
    config['db_dest_user']     = db_dest_user
    config['db_dest_pass']     = db_dest_pass
    config['db_sour_string']   = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_dest_string']   = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['transfer_sour_db'] = transfer_sour_db
    config['transfer_dest_db'] = transfer_dest_db    
    config['db_sour']          = get_conn(db_sour_ip,db_sour_port ,db_sour_service,db_sour_user,db_sour_pass)
    config['db_dest']          = get_conn(db_dest_ip,db_dest_port ,db_dest_service,db_dest_user,db_dest_pass)
    cr=config['db_sour'].cursor()
    cr.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
    cr.close()
    cr=config['db_dest'].cursor()
    cr.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
    cr.close()
    config['transfer_table']   = transfer_table
    config['transfer_tab_part_range']=transfer_tab_part_range
    return config

def exp_csv(config):
    cr = config['db'].cursor()
    file_name   = config['exp_query'].replace('.sql','.'+config['exp_type'])
    file_handle = open(file_name, 'w')
    cr.execute('use {0}'.format(config['database']))
    cr.execute(get_exp_query(config['exp_query']))
    rs = cr.fetchall()
    for i in rs:
        row = ''
        for j in range(len(i)):
            if i[j] is None:
                row = row + ','
            else:
                row = row + str(i[j])+','
        #print(row[0:-1])
        file_handle.write(row[0:-1]+'\n')
    config['db'].commit()
    cr.close()
    file_handle.close()
    print("{0} export complete!".format(file_name))

def exp_xls(config):
    workbook = xlwt.Workbook(encoding='utf8')
    worksheet = workbook.add_sheet('xls')
    cr = config['db'].cursor()
    file_name = config['exp_query'].replace('.sql', '.' + config['exp_type'])
    file_handle = open(file_name, 'w')
    cr.execute('use {0}'.format(config['database']))
    cr.execute(get_exp_query(config['exp_query']))
    rs = cr.fetchall()
    desc = cr.description
    #output header
    for k in range(len(desc)):
        worksheet.write(0, k, label=desc[k][0])
    #output content
    row=1
    for i in rs:
        for j in range(len(i)):
            if i[j] is None:
                worksheet.write(row, j, label='')
            else:
                worksheet.write(row, j, label=str(i[j]))
        row = row + 1
    workbook.save(file_name)
    config['db'].commit()
    cr.close()

    print("{0} export complete!".format(file_name))

def exp(fname):
    config = get_conf_exp(fname)
    print('MySQL exp parameter...')
    output_parameter(config)
    if config['exp_type'].lower()=="csv":
        exp_csv(config)
    if config['exp_type'].lower()=="xls":
        exp_xls(config)

def output_parameter(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))
 
def get_table_defi(db,owner,tab):
    cr=db.cursor()
    sql="""show create table {0}.{1}""".format(owner,tab)
    cr.execute(sql)
    rs=cr.fetchall()
    cr.close()
    return process_tab_defi(rs[0][1])

def process_tab_defi(cre_tab_sql):
    v_tmp=''
    if 'FOREIGN KEY' in cre_tab_sql:
       tab_lines=cre_tab_sql.split("\n")
       for i in cre_tab_sql.split("\n"):
         if 'CONSTRAINT' not in i :
            v_tmp=v_tmp+i+'\n'
       v_tmp=v_tmp[0:-1].split(') ENGINE')[0][0:-2]+'\n ) ENGINE'+v_tmp[0:-1].split(') ENGINE')[1]
    else:
       v_tmp=cre_tab_sql
    return v_tmp

def check_exists_tab(db,owner,tab):
   cr=db.cursor()
   sql="select count(0) from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(owner,tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_tab_partition(db,owner,tab):
   cr=db.cursor()
   sql="""select count(0) from information_schema.partitions where table_schema='{0}' and table_name='{1}'""".format(owner,tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_exists_pk(db,owner,tab):
   cr=db.cursor()
   sql="""select count(0) FROM information_schema.columns
           where table_schema='{0}' AND table_name='{1}' AND column_key='PRI'
       """.format(owner,tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_batch_size(n_tab_rows):
    n_batch_size=0
    if n_tab_rows >= 100000:
      n_batch_size=4000
    elif n_tab_rows >= 10000:
      n_batch_size=2000
    elif n_tab_rows >=5000:
      n_batch_size=500
    else:
      n_batch_size=n_tab_rows//2
    return n_batch_size

def get_tab_rows(db,user,tname):
    cr=db.cursor()
    sql="select count(0) from "+user+"."+tname
    cr.execute(sql)
    rs=cr.fetchone()
    n_tab_total_rows=rs[0]
    cr.close()
    return n_tab_total_rows

def get_part_tab_rows(config,tname):
    n_total_rows=0
    cr=config['db_sour'].cursor()
    v_part=''
    if config['transfer_tab_part_range']!='':
        v_part_begin=config['transfer_tab_part_range'].split('~')[0]
        v_part_end  =config['transfer_tab_part_range'].split('~')[1]    
        v_part="""select partition_name
                   from information_schema.partitions
                  where table_schema='{0}' 
                    and table_name='{1}' 
                    and partition_name between '{2}' and '{3}'
                  order by  partition_name""".format(config['transfer_sour_db'],tname,v_part_begin,v_part_end)
    else:
        v_part="""select partition_name
                   from information_schema.partitions
                  where table_schema='{0}' and table_name='{1}' ORDER BY partition_name""".format(user,tname)
    cr.execute(v_part)
    rs=cr.fetchall()
    for i in range(len(rs)):     
      v_sql="select count(0) from {0}.{1} partition({2})".format(config['transfer_sour_db'],tname,rs[i][0])
      cr.execute(v_sql)
      rs2=cr.fetchone()
      n_total_rows=n_total_rows+rs2[0]
    cr.close()
    config['db_sour'].commit()
    return n_total_rows

def get_tab_part_rows(db,user,tname,part):
    cr=db.cursor()
    sql="select count(0) from {0}.{1} partition({2})".format(user,tname,part)
    cr.execute(sql)
    rs=cr.fetchone()
    n_tab_total_rows=rs[0]
    cr.close()
    return n_tab_total_rows

def get_transfer_header(fromdb,fromuser,touser,tname):
    cr=fromdb.cursor()
    sql="select * from "+fromuser+"."+tname+" limit 1"
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+touser+"."+tname+"("
    s2=" values("
    for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','  
    s1=s1[0:-1]+')'
    cr.close()
    fromdb.commit()
    return s1+s2

def get_table_pk(config,tab):
    cr=config['db_sour'].cursor()
    sql="""SELECT column_name FROM information_schema.columns      
             WHERE table_schema='{0}' AND table_name='{1}' AND column_key='PRI'
        """.format(config['transfer_sour_db'],tab)
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]

def get_batch_keys(config,tab,n_begin,n_end):
    cr=config['db_sour'].cursor()
    col=get_table_pk(config,tab)
    sql="""select {0} from {1}.{2} order by {3} limit {4},{5}""".format(col,config['transfer_sour_db'],tab,col,n_begin,n_end)
    #print(sql)
    cr.execute(sql)
    rs=cr.fetchall()
    v_keys=''
    for i in rs:
        v_keys=v_keys+str(i[0])+','
    config['db_sour'].commit();
    cr.close()
    return v_keys[0:-1]    

def transfer_data(config,tname):
    n_tab_total_rows = get_tab_rows(config['db_sour'],config['transfer_sour_db'],tname)
    n_batch_size     = get_batch_size(n_tab_total_rows)
    ins_sql_header   = get_transfer_header(config['db_sour'],config['transfer_sour_db'],config['transfer_dest_db'],tname)
    t_syn_begin_time = datetime.datetime.now()
    print("*".ljust(85,"*"))   
    print("* Sour Connection".ljust(23,' ')+": "+config['db_sour_string'].ljust(59,' ')+"*")
    print("* Sour Database".ljust(23,' ')+": "+config['transfer_sour_db'].ljust(59,' ')+"*")
    print("* Dest Connection".ljust(23,' ')+": "+config['db_dest_string'].ljust(59,' ')+"*")
    print("* Dest Database".ljust(23,' ')+": "+config['transfer_dest_db'].ljust(59,' ')+"*")
    print("* Transfer table".ljust(23,' ')+": "+tname.ljust(59,' ')+"*")
    print("* Transfer rows".ljust(23,' ')+": "+str(n_tab_total_rows).ljust(59,' ')+"*")
    print("* Transfer batch".ljust(23,' ')+": "+str(n_batch_size).ljust(59,' ')+"*")
    print("*".ljust(85,"*"))
    if n_tab_total_rows!=0:
       from_cr=config['db_sour'].cursor()
       to_cr=config['db_dest'].cursor()
       n_begin=0
       from_sql="select * from "+config['transfer_sour_db']+"."+tname+" where "+get_table_pk(config,tname)+" in ("+get_batch_keys(config,tname,n_begin,n_batch_size)+")"
       from_cr.execute(from_sql)
       desc=from_cr.description 
       from_rs=from_cr.fetchall()
       ins_val=""
       i_counter=0
       print("Transfer Table..."+tname.upper()+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
       while True:
         for row in list(from_rs):
            ins_val=""
            for j in range(len(row)):
               col_type=str(desc[j][1])  
               if col_type=="253" or col_type=="252" or col_type=="12":
                  if row[j] is None:
                     ins_val=ins_val+"null,"
                  else: 
                     ins_val=ins_val+"'"+format_sql(str(row[j]))+"',"
               else:
                  if row[j] is None:
                     ins_val=ins_val+"null,"
                  else:
                     ins_val=ins_val+str(row[j])+","
            ins_sql=ins_sql_header+ins_val[0:-1]+');\n'
            i_counter=i_counter+1
            try:
               to_cr.execute(ins_sql)
            except:
               print(traceback.format_exc())
               print(ins_sql)
               sys.exit(0)

            if i_counter%n_batch_size==0:
               config['db_dest'].commit()
               print("\rTotal rec:{0},Process rec:{1},Complete:{2}%".format(n_tab_total_rows,i_counter,round(i_counter/n_tab_total_rows*100,2)),end='')

         if len(from_rs)<n_batch_size:
            break
         n_begin=n_begin+n_batch_size
         from_sql="select * from "+config['transfer_sour_db']+"."+tname+" where "+get_table_pk(config,tname)+" in ("+get_batch_keys(config,tname,n_begin,n_batch_size)+")"
         from_cr.execute(from_sql)  
         from_rs=from_cr.fetchall()
      
       config['db_dest'].commit()
       print("\rTotal rec:{0},Process rec:{1},Complete:{2}%".format(n_tab_total_rows,i_counter,round(i_counter/n_tab_total_rows*100,2)),end='') 
             
def transfer_data_partition(config,tname):
    n_tab_total_rows  = get_part_tab_rows(config,tname)
    n_batch_size      = get_batch_size(n_tab_total_rows)
    ins_sql_header    = get_transfer_header(config['db_sour'],config['transfer_sour_db'],config['transfer_dest_db'],tname)
    t_syn_begin_time  = datetime.datetime.now()
    print("*".ljust(85,"*"))
    print("* Sour Connection".ljust(23,' ')+": "+config['db_sour_string'].ljust(59,' ')+"*")
    print("* Sour Database".ljust(23,' ')+": "+config['transfer_sour_db'].ljust(59,' ')+"*")
    print("* Dest Connection".ljust(23,' ')+": "+config['db_dest_string'].ljust(59,' ')+"*")
    print("* Dest Database".ljust(23,' ')+": "+config['transfer_dest_db'].ljust(59,' ')+"*")
    print("* Transfer table".ljust(23,' ')+": "+tname.ljust(59,' ')+"*")
    if config['transfer_tab_part_range']!='':
       print("* Transfer partition".ljust(23,' ')+": "+config['transfer_tab_part_range'].ljust(59,' ')+"*")
    print("* Transfer rows".ljust(23,' ')+": "+str(n_tab_total_rows).ljust(59,' ')+"*")
    print("* Transfer batch".ljust(23,' ')+": "+str(n_batch_size).ljust(59,' ')+"*")
    print("*".ljust(85,"*"))
    if n_tab_total_rows!=0:
       from_cr=config['db_sour'].cursor()
       to_cr=config['db_dest'].cursor()
       from_partition=''
       if config['transfer_tab_part_range']!='':
           v_part_begin=config['transfer_tab_part_range'].split('~')[0]
           v_part_end  =config['transfer_tab_part_range'].split('~')[1]
           from_partition="""select partition_name
                             from information_schema.partitions 
                             where table_schema='{0}' 
                               and table_name='{1}' 
                               and partition_name between '{2}' and '{3}'    
                             order by partition_name
                          """.format(config['transfer_sour_db'],tname,v_part_begin,v_part_end)
       else:
           from_partition="""select partition_name
                             from information_schema.partitions
                             where table_schema='{0}' and table_name='{1}' ORDER BY partition_name
                          """.format(config['transfer_sour_db'],tname)  
       from_cr.execute(from_partition)
       from_rs_part=from_cr.fetchall()
       i_counter=0
       for p in range(len(from_rs_part)):
           n_begin=0
           from_sql="select * from {0}.{1} partition ({2}) limit {3},{4}".format(config['transfer_sour_db'],tname,from_rs_part[p][0],n_begin,n_batch_size)
           from_cr.execute(from_sql)
           desc=from_cr.description
           from_rs=from_cr.fetchall()
           if get_tab_part_rows(config['db_dest'],config['transfer_dest_db'],tname,from_rs_part[p][0])>0:
             print("\nTransfer Table..."+tname.upper()+",partition="+from_rs_part[p][0]+" exist data,skip it!")
             continue
           ins_val=""
           i_counter=0
           n_tab_part_rows=get_tab_part_rows(config['db_sour'],config['transfer_sour_db'],tname,from_rs_part[p][0])
           print("\nTransfer Table..."+tname.upper()+",partition="+from_rs_part[p][0]+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_part_rows)+"...")
           while True:
             for row in list(from_rs):
                ins_val=""
                for j in range(len(row)):
                   col_type=str(desc[j][1])
                   if col_type=="253" or col_type=="252" or col_type=="12":
                      if row[j] is None:
                         ins_val=ins_val+"null,"
                      else:
                         ins_val=ins_val+"'"+format_sql(str(row[j]))+"',"
                   else:
                      if row[j] is None:
                         ins_val=ins_val+"null,"
                      else:
                         ins_val=ins_val+str(row[j])+","

                ins_sql=ins_sql_header+ins_val[0:-1]+');\n'
                i_counter=i_counter+1
                try:
                   to_cr.execute(ins_sql)
                except:
                   print(traceback.format_exc())
                   print(ins_sql)
                   sys.exit(0)

                if i_counter%n_batch_size==0:
                   config['db_dest'].commit()
                   print("\rPartition {0} rec:{1},Process rec:{2},Complete:{3}%".format(from_rs_part[p][0],\
                            n_tab_part_rows,i_counter,round(i_counter/n_tab_part_rows*100,2)),end='')

             if len(from_rs)<n_batch_size:
                break

             n_begin=n_begin+n_batch_size             
             from_sql="select * from {0}.{1} partition ({2}) limit {3},{4}".format(config['transfer_sour_db'],tname,from_rs_part[p][0],n_begin,n_batch_size)
             from_cr.execute(from_sql)
             from_rs=from_cr.fetchall()

           config['db_dest'].commit()
       from_cr.close()
       to_cr.close()
       print("\rTotal rec:{0},Process rec:{1},Complete:{2}%".format(n_tab_total_rows,i_counter,round(i_counter/n_tab_total_rows*100,2)),end='')

def transfer(fname,debug):
    config=get_mon_conf_transfer(fname)
    cr_sour=config['db_sour'].cursor()
    cr_dest=config['db_dest'].cursor()
    print("MySQL transfer parameter...")
    if debug:
       output_parameter(config)
    for tab in config['transfer_table'].split(","):
      if check_exists_pk(config['db_sour'],config['transfer_sour_db'],tab)==0:
         print("The table {0}.{1} has no primary key!")
         sys.exit(0)
      if check_exists_tab(config['db_dest'],config['transfer_dest_db'],tab)==0:
        cre_tab=get_table_defi(config['db_sour'],config['transfer_sour_db'],tab)
        cr_dest.execute('use {0}'.format(config['transfer_dest_db']))
        cr_dest.execute(cre_tab) 
        print("Table definition {0}.{1} transfer success!".format(config['transfer_sour_db'],tab))
      else:
        print("Table {0}.{1} already exists!".format(config['transfer_sour_db'],tab))      

      if check_tab_partition(config['db_sour'],config['transfer_sour_db'],tab)==0:
        transfer_data(config,tab)
      else:
        transfer_data_partition(config,tab)

    config['db_sour'].commit()
    config['db_dest'].commit()
    cr_sour.close()
    cr_dest.close()

def get_mon_conf_alone(fname):
    config=configparser.ConfigParser()
    config.read(fname,encoding="utf-8-sig")
    d_config = {}
    d_config['db1_ip']                  = config.get("ALONE","mon_server").split(':')[0]
    d_config['db1_port']                = config.get("ALONE","mon_server").split(':')[1]
    d_config['db1_service']             = config.get("ALONE","mon_server").split(':')[2]
    d_config['db1_user']                = config.get("ALONE","mon_server").split(':')[3]
    d_config['db1_pass']                = config.get("ALONE","mon_server").split(':')[4]
    d_config['db1_string']              = d_config['db1_ip'] + ':' + d_config['db1_port']+'/'+ d_config['db1_service']
    d_config['mon_db']                  = config.get("ALONE", "mon_db")
    d_config['send_user']               = config.get("ALONE","send_mail_user")
    d_config['send_pass']               = config.get("ALONE","send_mail_pass")
    d_config['acpt_user']               = config.get("ALONE","acpt_mail_user")
    d_config['cc_user']                 = config.get("ALONE","cc_mail_user")
    d_config['mail_gap']                = config.get("ALONE","send_mail_gap")
    d_config['slow_query_time']         = config.get("ALONE","slow_query_time")
    d_config['trx_block_time']          = config.get("ALONE","trx_block_time")
    d_config['mon_time_period']         = config.get("ALONE","mon_time_period")
    d_config['tar_var_rows']            = config.get("ALONE", "tar_var_rows")
    d_config['project_name']            = config.get("ALONE", "project_name")+"["+config.get("ALONE", "project_type")+"]"
    d_config['slow_query_title']        = config.get("ALONE", "project_name") + "慢查询预警" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['trx_block_title']         = config.get("ALONE", "project_name") + "阻塞预警" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['tab_var_title']           = config.get("ALONE", "project_name") + "表变化预警" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['slow_query_top20_title']  = config.get("ALONE", "project_name") + "慢查询TOP20" + "[" + config.get("ALONE","project_type") + "]"
    d_config['db_health_title']         = config.get("ALONE", "project_name") + "健康检查" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['db_metadata_lock_title']  = config.get("ALONE", "project_name") + "元数据锁预警" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['db_proc_ddl_title']       = config.get("ALONE", "project_name") + "存储过程中含DDL语句预警" + "[" + config.get("ALONE","project_type") + "]"
    d_config['total_connections']       = config.get("ALONE", "total_connections")
    d_config['active_connections']      = config.get("ALONE", "active_connections")
    d_config['slow_query_numbers']      = config.get("ALONE", "slow_query_numbers")
    d_config['max_trans_numbers']       = config.get("ALONE", "max_trans_numbers")
    d_config['lock_enqueue_numbers']    = config.get("ALONE", "lock_enqueue_numbers")
    d_config['plugin_dir']              = config.get("ALONE", "plugin_dir")
    d_config['index_schema']            = config.get("REBUILD_INDEX", "index_schema")
    d_config['index_tables']            = config.get("REBUILD_INDEX", "index_tables")
    d_config['index_method']            = config.get("REBUILD_INDEX", "index_method")
    d_config['index_log']               = config.get("REBUILD_INDEX", "index_log")
    return d_config

def get_mon_conf_cluster(fname):
    config = configparser.ConfigParser()
    config.read(fname, encoding="utf-8-sig")
    d_config = {}
    d_config['db1_ip']      = config.get("CLUSTER", "mon_server1").split(':')[0]
    d_config['db1_port']    = config.get("CLUSTER", "mon_server1").split(':')[1]
    d_config['db1_service'] = config.get("CLUSTER", "mon_server1").split(':')[2]
    d_config['db1_user']    = config.get("CLUSTER", "mon_server1").split(':')[3]
    d_config['db1_pass']    = config.get("CLUSTER", "mon_server1").split(':')[4]
    d_config['db2_ip']      = config.get("CLUSTER", "mon_server2").split(':')[0]
    d_config['db2_port']    = config.get("CLUSTER", "mon_server2").split(':')[1]
    d_config['db2_service'] = config.get("CLUSTER", "mon_server2").split(':')[2]
    d_config['db2_user']    = config.get("CLUSTER", "mon_server2").split(':')[3]
    d_config['db2_pass']    = config.get("CLUSTER", "mon_server2").split(':')[4]
    d_config['db1_pass']    = config.get("ALONE", "mon_server").split(':')[4]
    d_config['db1_string']  = d_config['db1_ip'] + ':' + d_config['db1_port'] + '/' + d_config['db1_service']
    d_config['db2_string']  = d_config['db2_ip'] + ':' + d_config['db2_port'] + '/' + d_config['db2_service']
    d_config['mon_db']      = config.get("CLUSTER", "mon_db")
    d_config['send_user']   = config.get("CLUSTER", "send_mail_user")
    d_config['send_pass']   = config.get("CLUSTER", "send_mail_pass")
    d_config['acpt_user']   = config.get("CLUSTER", "acpt_mail_user")
    d_config['cc_user']     = config.get("CLUSTER", "cc_mail_user")
    d_config['mail_gap']    = config.get("CLUSTER", "send_mail_gap")
    d_config['slow_query_time']  = config.get("CLUSTER", "slow_query_time")
    d_config['trx_block_time']   = config.get("CLUSTER", "trx_block_time")
    d_config['sync_relay_time']  = config.get("CLUSTER", "sync_relay_time")
    d_config['mon_time_period']  = config.get("CLUSTER", "mon_time_period")
    d_config['tar_var_rows']     = config.get("CLUSTER", "tar_var_rows")
    d_config['project_name']     = config.get("CLUSTER", "project_name") + "[" + config.get("CLUSTER", "project_type") + "]"
    d_config['slow_query_title']        = config.get("CLUSTER", "project_name") + "慢查询预警" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['slow_query_title_slave']  = config.get("CLUSTER", "project_name") + "从库慢查询预警" + "[" + config.get("ALONE", "project_type") + "]"
    d_config['trx_block_title']         = config.get("CLUSTER", "project_name") + "阻塞预警" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['tab_var_title']           = config.get("CLUSTER", "project_name") + "表变化预警" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['slow_query_top20_title']  = config.get("CLUSTER", "project_name") + "慢查询TOP20" + "[" + config.get("CLUSTER", "project_type") + "]"
    d_config['db_health_title']         = config.get("CLUSTER", "project_name") + "健康检查" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['db_health_title_slave']   = config.get("CLUSTER", "project_name") + "健康检查-从库" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['db_metadata_lock_title']  = config.get("CLUSTER", "project_name") + "元数据锁预警" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['db_proc_ddl_title']       = config.get("CLUSTER", "project_name") + "存储过程中含DDL语句预警" + "[" + config.get("CLUSTER", "project_type") + "]"
    d_config['db_sync_relay_title']     = config.get("CLUSTER", "project_name") + "主从同步预警" + "[" + config.get("CLUSTER","project_type") + "]"
    d_config['total_connections']       = config.get("CLUSTER", "total_connections")
    d_config['active_connections']      = config.get("CLUSTER", "active_connections")
    d_config['slow_query_numbers']      = config.get("CLUSTER", "slow_query_numbers")
    d_config['max_trans_numbers']       = config.get("CLUSTER", "max_trans_numbers")
    d_config['lock_enqueue_numbers']    = config.get("CLUSTER", "lock_enqueue_numbers")
    d_config['plugin_dir']   = config.get("CLUSTER", "plugin_dir")
    d_config['index_schema'] = config.get("REBUILD_INDEX", "index_schema")
    d_config['index_tables'] = config.get("REBUILD_INDEX", "index_tables")
    d_config['index_method'] = config.get("REBUILD_INDEX", "index_method")
    d_config['index_log']    = config.get("REBUILD_INDEX", "index_log")
    return d_config

def check_logon_alone(config):
    times=0
    while True:
       try:
          db =get_conn(config['db1_ip'],config['db1_port'] ,config['db1_service'], config['db1_user'],config['db1_pass'])
          print("connect success!")
          config['db1']=db
          break
       except:
          times=times+1
          v_error = format_error(exception_info())
          v_title="MySQL数据库连接异常预警"+str(times)
          v_content=get_html_logon(config,v_error,times)
          print("check_logon_alone exception,times={0}!!!".format(str(times)))
          if times%5==0:
             send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],v_title,v_content)
             print("mail send success! times={0}".format(str(times)))
          time.sleep(60)

def check_logon_cluster(config):
    db1_times = 0
    db2_times = 0
    while True:
       try:
          db1 =get_conn(config['db1_ip'],config['db1_port'] ,config['db1_service'], config['db1_user'],config['db1_pass'])
          print("connect db1 success!")
          config['db1']=db1
       except:
          db1_times=db1_times+1
          v_error = format_error(exception_info())
          v_title="MySQL数据库连接异常预警"+str(db1_times)
          v_content=get_html_logon(config,v_error,db1_times)
          print("check_logon cluster  exception,times={0}!!!".format(str(db1_times))) 
          if db1_times%5==0:
             send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],v_title,v_content)
             print("mail send success! times={0}".format(str(db1_times)))
          time.sleep(60)

       try:
           db2 = get_conn(config['db2_ip'], config['db2_port'], config['db2_service'], config['db2_user'],config['db2_pass'])
           print("connect db2 success!")
           config['db2'] = db2
           break
       except:
           db2_times = db2_times + 1
           v_error = format_error(exception_info())
           v_title = "MySQL数据库连接异常预警" + str(db2_times)
           v_content = get_html_logon(config, v_error, db2_times)
           print("check_logon cluster exception,times={0}!!!".format(str(db2_times)))
           if db2_times % 5 == 0:
               send_mail(config['send_user'], config['send_pass'], config['acpt_user'],config['cc_user'],v_title, v_content)
               print("mail send success! times={0}".format(str(db2_times)))
           time.sleep(60)

def send_mail(p_from_user,p_from_pass,p_to_user,p_to_cc,p_title,p_content):
    to_user=p_to_user.split(",")
    cc = p_to_cc.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        msg["Cc"]      = ",".join(cc)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user+cc, msg.as_string())
        server.quit()
        return 0
    except smtplib.SMTPException as e:
        return -1

def send_mail_attch(p_from_user,p_from_pass,p_to_user,p_to_cc,p_title,p_content,p_attch):
    try:
        #设置登录及服务器信息
        sender    = p_from_user
        receivers = p_to_user.split(",")
        cc        = p_to_cc.split(",")
        #添加一个MIMEmultipart类，处理正文及附件
        message = MIMEMultipart('alternative')
        message['From'] = sender
        message['To'] = ",".join(receivers)
        message['Cc'] = ",".join(cc)
        message['Subject'] = p_title
        message['Date'] = formatdate()
        with open(p_attch,'rb')as fp:
             bfile = MIMEApplication(fp.read())
        bfile['Content-Type'] = 'application/octet-stream'
        bfile['Content-Disposition'] = 'attachment;filename="{0}"'.format(p_attch)
        message.attach(bfile)
        htm = MIMEText(p_content,'html','utf-8')
        message.attach(htm)
        fp.close()
        server = smtplib.SMTP()
        server.connect("smtp.exmail.qq.com",25)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com")
        server.login(p_from_user, p_from_pass)
        server.sendmail(sender, receivers+cc, message.as_string())
        print("send mail attch success!")
        server.quit()
        return 0
    except smtplib.SMTPException as e:
        print('error',e)
        return -1

def send_mail_attch2(p_from_user,p_from_pass,p_to_user,p_to_cc,p_title,p_content,p_files):
    try:
        #设置登录及服务器信息
        sender    = p_from_user
        receivers = p_to_user.split(",")
        cc        = p_to_cc.split(",")
        #添加一个MIMEmultipart类，处理正文及附件
        message = MIMEMultipart('alternative')
        message['From'] = sender
        message['To'] = ",".join(receivers)
        message['Cc'] = ",".join(cc)
        message['Subject'] = p_title
        message['Date'] = formatdate()
        #添加网页正文
        htm = MIMEText(p_content,'html','utf-8')
        message.attach(htm)
        #添加图片正文列表
        for i in range(len(p_files)):
           fp = open(p_files[i],'rb')
           msgImage = MIMEImage(fp.read())
           msgImage['Content-Type'] = 'application/octet-stream'
           msgImage['Content-Disposition'] = 'attachment;filename="{0}"'.format(p_files[i].split("/")[-1])
           fp.close()
           msgImage.add_header('Content-ID','<dba_image'+str(i)+'>')
           message.attach(msgImage)
           htm_img="""<div style="border=1px" class="image">
                         <img src="cid:dba_image{0}">
                      </div>                     
                   """.format(str(i))
           htm = MIMEText(htm_img,'html','utf-8')
           message.attach(htm)
           fp.close()

        server = smtplib.SMTP()
        server.connect("smtp.exmail.qq.com",25)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com")
        server.login(p_from_user, p_from_pass)
        server.sendmail(sender, receivers+cc, message.as_string())
        print("send mail attch image success!")
        server.quit()
        return 0
    except smtplib.SMTPException as e:
        print('error',e)
        return -1

def get_mon_period(config):
    flag=False
    if config['mon_time_period']=='*':
       return True
    if config['mon_time_period']=='!':
       return False
    d_current=int(datetime.datetime.now().strftime('%H'))    
    d_t_range=config['mon_time_period'].split(",")
    for i in range(len(d_t_range)):
      if d_current>=int(d_t_range[i].split("~")[0]) and d_current<=int(d_t_range[i].split("~")[1]):         
         return True
    return flag

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def monitor_alone(fname):
    i_counter =0
    #read configure
    config=get_mon_conf_alone(fname)
    #logon check
    check_logon_alone(config)
    #monitoring show sql and transaction
    start_time=datetime.datetime.now()
    start_time_slow_sql=datetime.datetime.now()
    start_time_meta_lock=datetime.datetime.now()
    start_time_db_info=datetime.datetime.now()
    start_time_db_health=datetime.datetime.now()
    start_time_db_health_success=datetime.datetime.now()
    start_time_proc_ddl=datetime.datetime.now()
    start_time_tab_var=datetime.datetime.now()
    start_time_slow_sql_hz=datetime.datetime.now()
    while True:
       #sleep 3 seconds
       time.sleep(1)

       # gather database item Once every three minutes
       if get_seconds(start_time_db_info) >= 600:
           gather_db_info(config)
           start_time_db_info = datetime.datetime.now()

       #gather slow sql for Once every one minutes
       if get_seconds(start_time_slow_sql)>=10:
           gather_slow_sql(config)
           start_time_slow_sql=datetime.datetime.now()
       
       #gather slow sql for Once every two minutes
       if get_seconds(start_time_meta_lock)>=3:
           gather_metadata_lock_info(config)
           start_time_meta_lock=datetime.datetime.now()
 
       #gather tab var every 1800 minutes
       if get_seconds(start_time_tab_var)>=1800:
           gather_tab_var_info(config)
           start_time_tab_var=datetime.datetime.now()
      
       #monitor period range send mail  
       if get_mon_period(config):
           i_counter = i_counter+1
           #time.sleep(1)
           #print('get_seconds=',get_seconds(),'start_time=',start_time)
           if get_seconds(start_time)>=int(config['mail_gap']):
              #slow query monitor
              v_content=get_slow_sql(config)
              if v_content!='':
                 send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],config['slow_query_title']+str(i_counter),v_content)
                 print("slow query mail send success!")

              #trx block monitor
              v_content=get_block_txn(config)
              if v_content!='':
                 send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],config['trx_block_title']+str(i_counter),v_content)
                 print("trx block mail send success!")

              #metadata lock monitor 
              v_content=get_metadata_lock(config)
              if v_content!='':
                 send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],config['db_metadata_lock_title']+str(i_counter),v_content)
                 print("metadata lock  mail send success!")           
              start_time=datetime.datetime.now()

           #every tow hours send top 20 slow sql
           if get_seconds(start_time_slow_sql_hz)>=7200:  
              v_content,v_file=send_slow_sql_hz(config)
              if v_content!='':
                 send_mail_attch(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],\
                                 config['slow_query_top20_title']+str(i_counter),v_content,v_file)
                 os.system('rm -f {0}'.format(v_file))
                 start_time_slow_sql_hz=datetime.datetime.now()
                 print("slow query hz mail send success!") 
        
           #every day 12 hours send table variable rate,43200 
           if get_seconds(start_time_tab_var)>=86400:
             v_content=send_tab_var_rate(config)
             if v_content!='':
                send_mail(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],config['tab_var_title']+str(i_counter),v_content)
                start_time_tab_var=datetime.datetime.now() 
                print("tab var rate mail send success!")
         
           #every half hour or go wrong at work send db health report
           n_status=0
           if get_seconds(start_time_db_health)>=300:
              start_time_db_health=datetime.datetime.now()
              if if_exceed_threshold(config):
                 v_db_health_title=config['db_health_title']+'*异常'
                 n_status=1
              else:
                 if get_seconds(start_time_db_health_success)>=180: #1800
                    v_db_health_title=config['db_health_title']+'*正常'
                    n_status=1
              if n_status==1:
                 #health check master
                 v_content,v_files=get_db_health(config)
                 send_mail_attch2(config['send_user'],config['send_pass'],config['acpt_user'],config['cc_user'],v_db_health_title+str(i_counter),v_content,v_files)
                 print("db master health check mail send success!")
                 for f in range(len(v_files)):   
                     os.system('rm -f {0}'.format(v_files[f]))
                 start_time_db_health=datetime.datetime.now()
                 start_time_db_health_success=datetime.datetime.now()


def monitor_cluster(fname):
    i_counter = 0
    #read configure
    config = get_mon_conf_cluster(fname)
    #logon check
    check_logon_cluster(config)
    #init variables
    start_time           = datetime.datetime.now()
    start_time_db_info   = datetime.datetime.now()
    start_time_slow_sql  = datetime.datetime.now()
    start_time_meta_lock = datetime.datetime.now()
    start_time_tab_var   = datetime.datetime.now()
    start_time_db_health = datetime.datetime.now()
    start_time_db_health_success = datetime.datetime.now()
    start_time_slow_sql_hz = datetime.datetime.now()
    while True:
        #sleep 3 seconds
        time.sleep(3)
        #gather database item Once every three minutes
        if get_seconds(start_time_db_info) >= 600:
           gather_db_info(config)
           gather_db_info_slave(config)
           start_time_db_info = datetime.datetime.now()

        #gather slow sql for Once every one minutes
        if get_seconds(start_time_slow_sql) >= 3:
           gather_slow_sql(config)
           gather_slow_sql_slave(config)
           start_time_slow_sql = datetime.datetime.now()

        #gather slow sql for Once every two minutes
        if get_seconds(start_time_meta_lock) >= 5:
           gather_metadata_lock_info(config)
           start_time_meta_lock = datetime.datetime.now()

        #gather tab var every 1800 minutes
        if get_seconds(start_time_tab_var) >= 1800:  #1800s
            gather_tab_var_info(config)
            start_time_tab_var = datetime.datetime.now()

        #monitor period range send mail
        if get_mon_period(config):
            i_counter = i_counter + 1
            if get_seconds(start_time) >= int(config['mail_gap']):
                #slow query monitor
                v_content = get_slow_sql(config)
                if v_content != '':
                   send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                             config['slow_query_title'] + str(i_counter), v_content)
                   print("db1 slow query mail send success!")

                v_content = get_slow_sql_slave(config)
                if v_content != '':
                   send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                             config['slow_query_title_slave'] + str(i_counter), v_content)
                   print("db2 slave slow query mail send success!")

                #trx block monitor
                v_content = get_block_txn(config)
                if v_content != '':
                    send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                              config['trx_block_title'] + str(i_counter), v_content)
                    print("db1 trx block mail send success!")

                #master slave relay monitor
                d_sync = check_sync_relay(config)
                if d_sync['master_slave_relay'] >= int(config['sync_relay_time']):
                    v_content = get_html_ms_sync(config, d_sync)
                    send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                              config['db_sync_relay_title'] + str(i_counter), v_content)
                    print("sync relay mail send success!")

                #metadata lock monitor
                v_content = get_metadata_lock(config)
                if v_content != '':
                    send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                              config['db_metadata_lock_title'] + str(i_counter), v_content)
                    print("db1 metadata lock  mail send success!")
                start_time = datetime.datetime.now()

            # every tow hours send top 20 slow sql
            if get_seconds(start_time_slow_sql_hz) >= 7200:
                v_content, v_file = send_slow_sql_hz(config)
                if v_content != '':
                    send_mail_attch(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'], \
                                    config['slow_query_top20_title'] + str(i_counter), v_content, v_file)
                    os.system('rm -f {0}'.format(v_file))
                    start_time_slow_sql_hz = datetime.datetime.now()
                    print("db1 slow query hz mail send success!")

            #every day 12 hours send table variable rate,43200
            if get_seconds(start_time_tab_var) >= 1800:
                v_content = send_tab_var_rate(config)
                print('start_time_tab_var=',v_content)
                if v_content != '':
                    send_mail(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                              config['tab_var_title'] + str(i_counter), v_content)
                    start_time_tab_var = datetime.datetime.now()
                    print("tab var rate mail send success!")

            #every half hour or go wrong at work send db health report
            n_status = 0
            v_db_health_title=''
            v_health_title_slave=''
            if get_seconds(start_time_db_health) >= 180:
                start_time_db_health = datetime.datetime.now()
                if if_exceed_threshold(config):
                    v_db_health_title     = config['db_health_title'] + '*异常'
                    v_health_title_slave  = config['db_health_title_slave'] + '*异常'
                    n_status = 1
                else:
                    if get_seconds(start_time_db_health_success) >= 14400:
                       v_db_health_title    = config['db_health_title'] + '*正常'
                       v_health_title_slave = config['db_health_title_slave'] + '*正常'
                       n_status = 1

                if n_status == 1:
                    #health check master
                    v_content, v_files = get_db_health(config)
                    send_mail_attch2(config['send_user'], config['send_pass'], config['acpt_user'], config['cc_user'],
                                     v_db_health_title + str(i_counter), v_content, v_files)
                    print("db health check mail send success!")
                    for f in range(len(v_files)):
                        os.system('rm -f {0}'.format(v_files[f]))
                    #health check slave
                    v_content, v_files = get_db_health_slave(config)
                    send_mail_attch2(config['send_user'], config['send_pass'], config['acpt_user'],
                                     config['cc_user'],
                                     v_health_title_slave + str(i_counter), v_content, v_files)
                    print("db slave health check mail send success!")
                    for f in range(len(v_files)):
                        os.system('rm -f {0}'.format(v_files[f]))
                    start_time_db_health = datetime.datetime.now()
                    start_time_db_health_success = datetime.datetime.now()


def check_sync_relay(config):
    d_sync={}
    cr_master=config['db1'].cursor()
    cr_master.execute("SELECT x,UNIX_TIMESTAMP(x) FROM heart limit 1")
    rs_master=cr_master.fetchone()
    d_sync['master_clock']=rs_master[0]
    d_sync['master_clock_second']=rs_master[1]
    cr_slave=config['db2'].cursor()
    cr_slave.execute("SELECT x,UNIX_TIMESTAMP(x) FROM heart limit 1")
    rs_slave=cr_slave.fetchone()
    d_sync['slave_clock']=rs_slave[0]
    d_sync['slave_clock_second']=rs_slave[1]
    d_sync['master_slave_relay']=d_sync['master_clock_second']-d_sync['slave_clock_second'] 
    config['db1'].commit()
    config['db2'].commit()
    cr_master.close()
    cr_slave.close() 
    return d_sync

def get_conn_info(config):
    cr = config['db1'].cursor()
    p_conn_total_sql  = "SELECT count(0) FROM information_schema.processlist"
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_total_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    config['db1'].commit()
    cr.close()
    return "total:{0},active:{1}".format(p_conn_total,p_conn_active)

def get_conn_numbers(config):
    cr = config['db1'].cursor()
    p_conn_total_sql  = "SELECT count(0) FROM information_schema.processlist"
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_total_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    config['db1'].commit()
    cr.close()
    return p_conn_total,p_conn_active

def get_conn_numbers_slave(config):
    cr = config['db2'].cursor()
    p_conn_total_sql  = "SELECT count(0) FROM information_schema.processlist"
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_total_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    cr.close()
    return p_conn_total,p_conn_active

def db_to_html(str):
    return str.replace('\t','&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;').replace('\n','<br>')

def send_tab_var_rate(config):
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    if config['mon_db'] == '*':
        p_sql = """select a.TABLE_NAME as "表名",
                          a.table_rows as "表行数",
                          a.table_size as "表大小(MB)",
                          a.tjrq       as "统计日期",
                          b.table_rows as "表行数",
                          b.table_size as "表大小(MB)",
                          b.tjrq       as "统计日期",         
                         (b.table_rows-a.table_rows) AS "变化量(记录)",
                         (b.table_size-a.table_size) AS "变化量(大小)",
                          CONCAT((b.table_rows-a.table_rows) / a.table_rows*100,'%') AS "变化率(记录)",
                          CONCAT((b.table_size-a.table_size) / a.table_size*100,'%')  AS "变化率(大小)"
                    FROM (SELECT * FROM devops.t_tab_rate 
                           WHERE tjrq=(SELECT MIN(tjrq) FROM devops.t_tab_rate 
                                       WHERE DATE_FORMAT(tjrq, '%Y%m%d') =DATE_FORMAT(date_sub(curdate(),interval 1 day), '%Y%m%d')) ) a,
                         (SELECT * FROM devops.t_tab_rate 
                           WHERE tjrq=(SELECT MAX(tjrq) FROM devops.t_tab_rate 
                                       WHERE DATE_FORMAT(tjrq, '%Y%m%d') =DATE_FORMAT(curdate(), '%Y%m%d'))) b
                    where a.schema_name=b.schema_name  and a.TABLE_NAME=b.TABLE_NAME 
                """
    else:
        p_sql = """select a.TABLE_NAME as "表名",
                          a.table_rows as "表行数",
                          a.table_size as "表大小(MB)",
                          a.tjrq       as "统计日期",
                          b.table_rows as "表行数",
                          b.table_size as "表大小(MB)",
                          b.tjrq       as "统计日期",
                          (b.table_rows-a.table_rows) AS "变化量(记录)",
                          (b.table_size-a.table_size) AS "变化量(大小)",
                          CONCAT((b.table_rows-a.table_rows) / a.table_rows*100,'%') AS "变化率(记录)",
                          CONCAT((b.table_size-a.table_size) / a.table_size*100,'%')  AS "变化率(大小)"
                    FROM (SELECT * FROM devops.t_tab_rate
                           WHERE tjrq=(SELECT MIN(tjrq) FROM devops.t_tab_rate 
                                       WHERE DATE_FORMAT(tjrq, '%Y%m%d') =DATE_FORMAT(date_sub(curdate(),interval 1 day), '%Y%m%d')) ) a,
                         (SELECT * FROM devops.t_tab_rate
                           WHERE tjrq=(SELECT MAX(tjrq) FROM devops.t_tab_rate 
                                       WHERE DATE_FORMAT(tjrq, '%Y%m%d') =DATE_FORMAT(curdate(), '%Y%m%d'))) b
                    where a.schema_name=b.schema_name  and a.TABLE_NAME=b.TABLE_NAME and instr('{0}',a.schema_name)>0
                """.format(config['mon_db'])
    cr.execute(p_sql)
    rs=cr.fetchall()
    desc = cr.description
    print("rs=",len(rs))
    #检测不到慢SQL直接返回空串
    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0: 
           row=row+'<th bgcolor=#8E8E8E width=12%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=8%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        else:
          if i[7]!=0:
             row=row+'<td><font color="red">'+db_to_html(str(i[j]))+'</font></td>'
          else:      
             row=row+'<td>'+db_to_html(str(i[j]))+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row
    v_html =get_html_contents(config,thead,tbody)
    config['db1'].commit()
    cr.close()
    return v_html  

def get_slow_sql(config):
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    if config['mon_db'] == '*':
        p_sql = """Select  cast(id as char)     as "线程ID",
                           HOST                 as "访问者IP",
                           db                   as "数据库",
                           command              as "命令类型",
                           cast(TIME as char)   as "耗时(s)" ,
                           state                as "状态",
                           info                 as "语句"
                    From information_schema.processlist  
                    Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                      and instr(info,'SQL_NO_CACHE')=0
                      and time>={0}  order by time desc
                """.format(config['slow_query_time'])
    else:
        p_sql = """Select cast(id as char)      as "线程ID",
                          HOST                  as "访问者IP",
                          db                    as "数据库",
                          command               as "命令类型",
                          cast(TIME as char)    as "耗时(s)" ,
                          state                 as "状态",
                          info                  as "语句"
                   From information_schema.processlist  
                   Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                     and instr(info,'SQL_NO_CACHE')=0 
                     and time>={0} and instr('{1}', db)>0 order by time desc
                 """.format(config['slow_query_time'], config['mon_db'])
    cr.execute(p_sql)    
    rs=cr.fetchall()
    desc = cr.description

    # 检测不到慢SQL直接返回空串
    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0:
           row=row+'<th bgcolor=#8E8E8E width=5%>'+str(desc[k][0])+'</th>'
        elif k in(1,2,3,4,5):
           row=row+'<th bgcolor=#8E8E8E width=10%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=45%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:   
      row='<tr>'   
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>' 
        else:
          row=row+'<td>'+db_to_html(i[j])+'</td>' 
      row=row+'</tr>\n'
      tbody=tbody+row
    v_html =get_html_contents(config,thead,tbody)
    config['db1'].commit()
    cr.close()
    return v_html

def get_slow_sql_slave(config):
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db2'].cursor()
    if config['mon_db'] == '*':
        p_sql = """Select  cast(id as char)     as "线程ID",
                           HOST                 as "访问者IP",
                           db                   as "数据库",
                           command              as "命令类型",
                           cast(TIME as char)   as "耗时(s)" ,
                           state                as "状态",
                           info                 as "语句"
                    From information_schema.processlist  
                    Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                      and instr(info,'SQL_NO_CACHE')=0
                      and time>={0}  order by time desc
                """.format(config['slow_query_time'])
    else:
        p_sql = """Select cast(id as char)      as "线程ID",
                          HOST                  as "访问者IP",
                          db                    as "数据库",
                          command               as "命令类型",
                          cast(TIME as char)    as "耗时(s)" ,
                          state                 as "状态",
                          info                  as "语句"
                   From information_schema.processlist  
                   Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                     and instr(info,'SQL_NO_CACHE')=0 
                     and time>={0} and instr('{1}', db)>0 order by time desc
                 """.format(config['slow_query_time'], config['mon_db'])
    cr.execute(p_sql)
    rs=cr.fetchall()
    desc = cr.description

    # 检测不到慢SQL直接返回空串
    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0:
           row=row+'<th bgcolor=#8E8E8E width=5%>'+str(desc[k][0])+'</th>'
        elif k in(1,2,3,4,5):
           row=row+'<th bgcolor=#8E8E8E width=10%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=45%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        else:
          row=row+'<td>'+db_to_html(i[j])+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row
    v_html =get_html_contents_slave(config,thead,tbody)
    cr.close()
    return v_html

def get_metadta_lock_type(v_sql):
    v_ret=''
    if "create" in v_sql.lower() or "truncate" in v_sql.lower() or "alter" in v_sql.lower() or "drop" in v_sql.lower():
        v_ret='Exclusive Metadata Lock'
    else:   
        v_ret='Share Metadata Lock'
    return v_ret

def get_metadata_lock(config):
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    sql="""select count(0) from devops.t_metadata_lock where send_mail='N'"""
    cr.execute(sql)
    rs=cr.fetchone()
    if rs[0]==0:
      return ''
    sql="""SELECT 
              CAST(id AS CHAR)          AS "线程ID",
              HOST                      AS "访问者IP",
              db                        AS "数据库",
              command                   AS "命令类型",
              CAST(start_time AS CHAR)  AS "执行时间",
              CAST(TIME AS CHAR)        AS "耗时(s)" , 
              state                     AS "事件",
              STATUS                    AS "状态",
              lock_type                 AS "锁类型",
              info                      AS "语句"
          FROM devops.t_metadata_lock  
          WHERE send_mail='N'           
          ORDER BY db,time desc,lock_type"""   
    cr.execute(sql)
    rs=cr.fetchall()
    desc = cr.description
    row='<tr>'
    for k in range(len(desc)):
        if k in(0,2,3,5,7):
           row=row+'<th bgcolor=#8E8E8E width=4%>'+str(desc[k][0])+'</th>'
        elif k in(1,):
           row=row+'<th bgcolor=#8E8E8E width=8%>'+str(desc[k][0])+'</th>'
        elif k in(4,6,8,):
           row=row+'<th bgcolor=#8E8E8E width=12%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=36%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row
    
    ids=''
    for i in rs:
      row='<tr>'
      ids=ids+i[0]+','
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        elif j in(0,1,2,3,4,5,6,7,8):
          row=row+'<td>'+i[j]+'</td>'
        else:
          row=row+'<td>'+db_to_html(i[j])+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row
    v_html =get_html_contents(config,thead,tbody)
    v_upd_sql="update devops.t_metadata_lock set send_mail='Y' where id in({0})".format(ids[0:-1])
    cr.execute(v_upd_sql)
    #print("update t_metadata_lock send_mail is ok!".format(ids[0:-1]))
    config['db1'].commit()
    cr.close()
    return v_html    

def write_slow_sql_hz_xls(config):
    workbook = xlwt.Workbook(encoding = 'utf8')
    worksheet = workbook.add_sheet('top300')
    cr = config['db1'].cursor()
    p_sql="""SELECT   concat((@rowNum:=@rowNum+1),'') as "序号",x.*
             FROM(SELECT
                       host                       as "服务器", 
                       db                         as "数据库",
                       MAX(ESALPSE_TIME)          as "最大时长",
                       MIN(ESALPSE_TIME)          as "最小时长",
                       max(id)                    as "平均时长",
                       max(id)                    as "执行次数",
                       DATE_FORMAT(MIN(created_date),'%H:%i:%s')  as "最早时间",
                       DATE_FORMAT(MAX(created_date),'%H:%i:%s')  as "最近时间",
                       CASE
                           WHEN  MAX(ESALPSE_TIME)>=600  OR AVG(ESALPSE_TIME)>=200  OR COUNT(0)>=60 THEN '★★★★★'
                           WHEN  MAX(ESALPSE_TIME)>=400  OR AVG(ESALPSE_TIME)>=100  OR COUNT(0)>=30 THEN '★★★★'
                           WHEN  MAX(ESALPSE_TIME)>=200  OR AVG(ESALPSE_TIME)>=50   OR COUNT(0)>=10 THEN '★★★'
                           WHEN  MAX(ESALPSE_TIME)>=100  OR AVG(ESALPSE_TIME)>=30   OR COUNT(0)>=5  THEN '★★'
                           WHEN  MAX(ESALPSE_TIME)>=50   OR AVG(ESALPSE_TIME)>=15   OR COUNT(0)>=2  THEN '★'
                           ELSE  '☆'
                      END AS "优先级" ,
                      STATEMENT  AS "优化建议",
                      STATEMENT  AS "执行语句"
                FROM devops.t_slow_sql,(select (@rowNum:=0)) b
                where created_date BETWEEN CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 0:0:0') 
                   AND CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 23:59:59')
                   AND instr(statement,'SQL_NO_CACHE')=0
                GROUP BY substr(host,1,instr(host,':')-1),db,statement
                 HAVING MAX(ESALPSE_TIME)>{0}
                ORDER BY LENGTH(CASE
                  WHEN  MAX(ESALPSE_TIME)>=600  OR AVG(ESALPSE_TIME)>=200  OR COUNT(0)>=60 THEN '★★★★★'
                  WHEN  MAX(ESALPSE_TIME)>=400  OR AVG(ESALPSE_TIME)>=100  OR COUNT(0)>=30 THEN '★★★★'
                  WHEN  MAX(ESALPSE_TIME)>=200  OR AVG(ESALPSE_TIME)>=50   OR COUNT(0)>=10 THEN '★★★'
                  WHEN  MAX(ESALPSE_TIME)>=100  OR AVG(ESALPSE_TIME)>=30   OR COUNT(0)>=5  THEN '★★'
                  WHEN  MAX(ESALPSE_TIME)>=50   OR AVG(ESALPSE_TIME)>=15   OR COUNT(0)>=2  THEN '★'
                  ELSE  '☆'
                  END) DESC  ,1 DESC LIMIT 300) x
          """.format(config['slow_query_time'])
    cr.execute(p_sql)
    rs=cr.fetchall()
    if len(rs)==0:
      return ''
    
    desc = cr.description
    row='<tr>'
    for k in range(len(desc)):
        worksheet.write(0, k, label = desc[k][0])

    row=1
    for i in rs:
      for j in range(len(i)):
        if i[j] is None:
          worksheet.write(row, j, label = '')
        elif j in (5,):
          worksheet.write(row, j, label =str(round(int(get_exec_total_time(config,i[j]))/int(get_exec_times(config,i[j])),0)))             
        elif j in (6,):
          worksheet.write(row, j, label = get_exec_times(config,i[j]))
        elif j in (10,):
          worksheet.write(row, j, label = get_advice(str(i[j])[0:2000]))   
        else:
          worksheet.write(row, j, label = str(i[j])[0:2000])
      row=row+1
    file_name='top300_show_log_{0}.xls'.format(datetime.datetime.now().strftime('%Y%m%d%H%M'))
    zip_name ='top300_show_log_{0}.zip'.format(datetime.datetime.now().strftime('%Y%m%d%H%M'))
    workbook.save(file_name)
    os.system('zip -r {0} {1}'.format(zip_name,file_name)) 
    os.system('rm -f {0}'.format(file_name))
    config['db1'].commit
    cr.close()
    return zip_name

def get_advice(p_sql):
    v_str=''
    i_counter=0;
    if "UNION" in p_sql.upper() or "UNION ALL" in p_sql.upper():
       i_counter=i_counter+1
       v_str=v_str+'{0}.将查询中UNION或UNION ALL 连接的查询分别插入临时表\n'.format(i_counter)
    if "V_" in p_sql.upper() :
       i_counter=i_counter+1
       v_str=v_str+'{0}.请将查询中的视图改为从表直接查询\n'.format(i_counter)
    if "DISTINCT" in p_sql.upper() :
       i_counter=i_counter+1
       v_str=v_str+'{0}.请消除查询中的DISTINCT关键字\n'.format(i_counter)
    return v_str;  

def get_advice_html(p_sql):
    return get_advice(p_sql).replace("\n","<br>")

def get_exec_times(config,p_id):
    cr = config['db1'].cursor()
    p_sql="""SELECT id,esalpse_time
              FROM devops.t_slow_sql
              where created_date BETWEEN CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 0:0:0') AND CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 23:59:59')
                  AND statement=(select statement from devops.t_slow_sql where id={0})
               order by created_date
          """.format(int(p_id))
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_time=0
    i_counter=0;
    for i in rs:
       if i[1]>n_time:
          n_time=i[1]
          pass
       else:
          i_counter=i_counter+1
    config['db1'].commit()
    cr.close()
    return str(i_counter+1) 

def get_exec_total_time(config,p_id):
    cr = config['db1'].cursor()
    p_sql="""SELECT id,esalpse_time
              FROM devops.t_slow_sql
              where created_date BETWEEN CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 0:0:0') AND CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 23:59:59')
                  AND statement=(select statement from devops.t_slow_sql where id={0})
               order by created_date
          """.format(int(p_id))
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_time=0
    n_total_time=0
    for i in rs:
       if i[1]>n_time:
          n_time=i[1]
       else:
          n_total_time=n_total_time+n_time
    config['db1'].commit()
    cr.close()
    return str(n_total_time)

def get_slow_sql_tjsj(config):
    cr = config['db1'].cursor()
    p_sql='''select CONCAT(CONCAT(DATE_FORMAT(NOW(),'%Y-%m-%d'),' 0:0:0'),'~',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s'))'''
    cr.execute(p_sql)
    rs=cr.fetchone()
    config['db1'].commit()
    cr.close()
    return rs[0] 

def send_slow_sql_hz(config):
    row=''
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    p_sql="""SELECT  concat((@rowNum:=@rowNum+1),'') as "序号",x.* 
             FROM(SELECT
                       host                             as "服务器",
                       db                               as "数据库",
                       MAX(ESALPSE_TIME)                as "最大时长",
                       MIN(ESALPSE_TIME)                as "最小时长",
                       MAX(id)                          as "平均时长",
                       max(id)                          as "执行次数",
                       DATE_FORMAT(MIN(created_date),'%H:%i:%s')  as "最早时间",
                       DATE_FORMAT(MAX(created_date),'%H:%i:%s')  as "最近时间",
                       CASE  
                           WHEN  MAX(ESALPSE_TIME)>=600  OR AVG(ESALPSE_TIME)>=200  OR COUNT(0)>=60 THEN '★★★★★'
                           WHEN  MAX(ESALPSE_TIME)>=400  OR AVG(ESALPSE_TIME)>=100  OR COUNT(0)>=30 THEN '★★★★'
                           WHEN  MAX(ESALPSE_TIME)>=200  OR AVG(ESALPSE_TIME)>=50   OR COUNT(0)>=10 THEN '★★★'
                           WHEN  MAX(ESALPSE_TIME)>=100  OR AVG(ESALPSE_TIME)>=30   OR COUNT(0)>=5  THEN '★★' 
                           WHEN  MAX(ESALPSE_TIME)>=50   OR AVG(ESALPSE_TIME)>=15   OR COUNT(0)>=2  THEN '★' 
                           ELSE  '☆' 
                      END AS "优先级" ,
                      STATEMENT  AS "优化建议",
                      STATEMENT  AS "执行语句"
                FROM devops.t_slow_sql,(select (@rowNum:=0)) b
                where created_date BETWEEN CONCAT(DATE_FORMAT(now(),'%Y-%m-%d'),' 0:0:0') 
                    AND concat(DATE_FORMAT(now(),'%Y-%m-%d'),' 23:59:59')
                    AND instr(statement,'SQL_NO_CACHE')=0
                GROUP BY substr(host,1,instr(host,':')-1),db,statement 
                 HAVING MAX(ESALPSE_TIME)>{0}
                ORDER BY LENGTH(CASE 
                  WHEN  MAX(ESALPSE_TIME)>=600  OR AVG(ESALPSE_TIME)>=200  OR COUNT(0)>=60 THEN '★★★★★'
                  WHEN  MAX(ESALPSE_TIME)>=400  OR AVG(ESALPSE_TIME)>=100  OR COUNT(0)>=30 THEN '★★★★'
                  WHEN  MAX(ESALPSE_TIME)>=200  OR AVG(ESALPSE_TIME)>=50   OR COUNT(0)>=10 THEN '★★★'
                  WHEN  MAX(ESALPSE_TIME)>=100  OR AVG(ESALPSE_TIME)>=30   OR COUNT(0)>=5  THEN '★★' 
                  WHEN  MAX(ESALPSE_TIME)>=50   OR AVG(ESALPSE_TIME)>=15   OR COUNT(0)>=2  THEN '★' 
                  ELSE  '☆'
                  END) DESC  ,1 DESC LIMIT 20) x
          """.format(config['slow_query_time'])
    cr.execute(p_sql)
    rs=cr.fetchall()
    if len(rs)==0:
      return '',''
    desc = cr.description
    row='<tr>'
    for k in range(len(desc)):
        if k in(0,3,4,5,6,7,8,9,10):
           row=row+'<th bgcolor=#8E8E8E width=5%>'+desc[k][0]+'</th>'
        elif k in(1,):
           row=row+'<th bgcolor=#8E8E8E width=18%>'+desc[k][0]+'</th>'
        elif k in(2,):
           row=row+'<th bgcolor=#8E8E8E width=10%>'+desc[k][0]+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=30%>'+desc[k][0]+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        elif j in(5,):
          row=row+'<td>'+str(round(int(get_exec_total_time(config,i[j]))/int(get_exec_times(config,i[j])),0))+'</td>'
        elif j in(6,):
          row=row+'<td>'+get_exec_times(config,i[j])+'</td>'
        elif j in(10,):
          row=row+'<td>'+get_advice_html(str(i[j]))+'</td>'
        else:
          row=row+'<td>'+db_to_html(str(i[j]))+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row
    v_html = get_html_contents(config, thead, tbody)
    config['db1'].commit()
    cr.close()
    v_file=write_slow_sql_hz_xls(config)
    return v_html,v_file
	
def get_block_txn(config):
    row=''
    tbody=''
    thead=''
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    p_sql = """SELECT distinct 
                      b.trx_id      AS '事务ID',
                      a.id          AS '线程ID',
                      a.host        AS '主机',
                      a.db          AS '数据库',
                      a.user        AS '用户',
                      IFNULL(e.block_trx_id,'') AS '阻塞事务ID',
                      b.trx_started AS '事务开始时间',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         a.time 
                      ELSE 
                        TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)  END AS '时长(s)',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         'WAITTING' 
                      ELSE 'BLOCKING'  END AS '事务状态',
                      a.state       AS '进程状态',
                      b.trx_isolation_level AS '隔离级别',
                      c.lock_table  AS '锁定表',
                      c.lock_mode   AS '锁模式',
                      c.lock_type   AS '锁类型',
                      b.trx_query   AS 'SQL语句'
		FROM information_schema.processlist a
		INNER JOIN information_schema.innodb_trx  b ON b.trx_mysql_thread_id=a.id
		INNER JOIN information_schema.INNODB_LOCKS c ON b.trx_id=c.lock_trx_id
		LEFT  JOIN (SELECT w.requesting_trx_id,
					GROUP_CONCAT(CONCAT(w.blocking_trx_id,'|') ORDER BY t.trx_started  SEPARATOR '') AS block_trx_id,
					GROUP_CONCAT(CONCAT('kill ',t.trx_mysql_thread_id,'; \n') ORDER BY t.trx_started  SEPARATOR '') AS kill_thread
	     		    FROM information_schema.innodb_lock_waits w,
				 information_schema.innodb_trx  t
			    WHERE w.blocking_trx_id=t.trx_id
			    GROUP BY w.requesting_trx_id) e ON b.trx_id=e.requesting_trx_id
                WHERE  CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN
                           a.time
                       ELSE
                           TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)
                       END >={0}     
	         ORDER BY b.trx_started
            """.format(config['trx_block_time'])

    try:
       cr = config['db1'].cursor()
       cr.execute(p_sql)
       rs=cr.fetchall()

       #检测不到阻塞事务直接返回空串
       if len(rs)==0:
         return ''

       desc = cr.description
       row='<tr>'
       for k in range(len(desc)):
          if k in(0,1,4,5):         
             row=row+'<th bgcolor=#8E8E8E width=4%>'+desc[k][0]+'</th>'
          elif k in(2,3,7,11,12):
             row=row+'<th bgcolor=#8E8E8E width=6%>'+desc[k][0]+'</th>'
          elif k in(6,):
             row=row+'<th bgcolor=#8E8E8E width=14%>'+desc[k][0]+'</th>'
          elif k in(8,9,10):
             row=row+'<th bgcolor=#8E8E8E width=10%>'+desc[k][0]+'</th>'
          else:
             row=row+'<th bgcolor=#8E8E8E width=24%>'+db_to_html(desc[k][0])+'</th>'
       row=row+'</tr>'
       thead=thead+row

       for i in rs:
         row='<tr>'
         for j in range(len(i)):
           if i[j] is None:
             row=row+'<td>&nbsp;</td>'
           else:
             row=row+'<td>'+str(i[j])+'</td>'
         row=row+'</tr>\n'
         tbody=tbody+row

       v_html = get_html_contents(config, thead, tbody)
       config['db1'].commit()
       cr.close()
       return v_html
    except:
       return ''

def gather_slow_sql(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    #slow sql write to table
    if config['mon_db'] == '*':
       v_sql="""insert into devops.t_slow_sql(STATEMENT,esalpse_time,created_date,db,host)
                 select info,TIME,now(),db,'{0}' from information_schema.processlist
                 Where command NOT IN('Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                      and INSTR(info,'t_slow_sql')=0 AND  INSTR(info,'commit')=0 
                      and time>={1}
             """.format(config['db1_ip'],config['slow_query_time'])
    else:
       v_sql="""insert into devops.t_slow_sql(STATEMENT,esalpse_time,created_date,db,host)
                 select info,TIME,now(),db,'{0}' from information_schema.processlist
                 Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                   and info not like '%t_slow_sql%'
                   and info not like '%commit%'
                   and time>=0 and instr('{1}',db)>0
              """.format(config['db1_ip'],config['mon_db'])
    cr.execute(v_sql)
    config['db1'].commit()
    cr.close()
    print("gather slow sql:"+nowTime)

def gather_slow_sql_slave(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    cr2= config['db2'].cursor()
    v_sql=''
    if config['mon_db'] == '*':
        v_sql = """select info,TIME,now(),db,'{0}' from information_schema.processlist
                     Where command NOT IN('Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                          and instr(info,'t_slow_sql')=0 
                          and instr(info,'commit')=0 
                          and time>={1}
                 """.format(config['db2_ip'],config['slow_query_time'])
    else:
        v_sql = """select info,TIME,now(),db,'{0}' from information_schema.processlist
                     Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')
                       and instr(info,'t_slow_sql')=0 
                       and instr(info,'commit')=0 
                       and time>=0 and instr('{1}',db)>0
                  """.format(config['db2_ip'],config['mon_db'])

    cr2.execute(v_sql)
    rs2=cr2.fetchall()
    for i in range(len(rs2)):
       v_ins_sql="""insert into devops.t_slow_sql(STATEMENT,esalpse_time,created_date,db,host)
                     values('{0}','{1}','{2}','{3}','{4}')
                 """.format(format_sql(rs2[i][0]),rs2[i][1],rs2[i][2],rs2[i][3],rs2[i][4])
       print(v_ins_sql)
       cr.execute(v_ins_sql)
    config['db1'].commit()
    cr.close()
    cr2.close()
    print("gather slave slow sql:"+nowTime)

def gather_db_info(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    connection_total,connection_active=get_conn_numbers(config)
    v_sql="""insert into devops.t_db_monitor(connection_total,connection_active,tjrq,host) 
                         values({0},{1},now(),'{2}')""".format(connection_total,connection_active,config['db1_ip'])
    cr.execute(v_sql)
    config['db1'].commit()
    cr.close()
    print("gather db info:"+nowTime)

def gather_db_info_slave(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    connection_total,connection_active=get_conn_numbers_slave(config)
    v_sql="""insert into devops.t_db_monitor(connection_total,connection_active,tjrq,host) 
                         values({0},{1},now(),'{2}')""".format(connection_total,connection_active,config['db2_ip'])
    cr.execute(v_sql)
    config['db1'].commit()
    cr.close()
    print("gather db slave info:"+nowTime)

def gather_tab_var_info(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    v_sql="""insert into devops.t_tab_rate(schema_name,table_name,table_rows,table_size,tjrq)
               select   table_schema,
                        table_name,
                        table_rows,
                        convert(round(DATA_LENGTH/1024/1024+INDEX_LENGTH/1024/1024,0),SIGNED) AS table_size,
                        DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s') as TJRQ
               from information_schema.tables
                 where table_rows>={0} order by table_rows desc
           """.format(config['tar_var_rows'])
    cr.execute(v_sql)
    config['db1'].commit()
    cr.close()
    print("gather db1 tab var info:"+nowTime)

def set_block_tables(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    cr.execute("truncate table devops.t_metadata_lock_table")
    sql="""insert into devops.t_metadata_lock_table(db,tname)
           SELECT DISTINCT p.db,t.table_name FROM information_schema.processlist p ,information_schema.tables t
           WHERE p.state='Waiting for table metadata lock'
             AND t.table_schema=p.db
             AND INSTR(p.info,t.table_name)>0"""
    cr.execute(sql)
    config['db1'].commit()
    cr.close()
    print("set block tables ok!"+nowTime)

def gather_metadata_lock_info(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr = config['db1'].cursor()
    sql="SELECT count(0) FROM information_schema.processlist  WHERE state='Waiting for table metadata lock'"
    cr.execute(sql)
    rs=cr.fetchone()
    if rs[0]>0:
       set_block_tables(config) 
       sql="""INSERT INTO devops.t_metadata_lock(id,user,host,db,command,time,state,info,status,lock_type,send_mail,start_time,gather_time)
              SELECT id,user,host,db,command,time,state,info,'Waiting',
                     case when instr(lower(info),'create')>0 or instr(lower(info),'truncate')>0 or instr(lower(info),'alter')>0 or instr(lower(info),'drop')>0 then
                        'Exclusive Metadata Lock'
                     else
                        'Share Metadata Lock'
                     end,  
                     'N',DATE_ADD(NOW(),INTERVAL-TIME SECOND),'{0}'
               FROM information_schema.processlist 
                 WHERE state='Waiting for table metadata lock'
              UNION ALL
              SELECT id,user,host,db,command,time,state,info,'blocking',
                     case when instr(lower(info),'create')>0 or instr(lower(info),'truncate')>0 or instr(lower(info),'alter')>0 or instr(lower(info),'drop')>0 then
                        'Exclusive Metadata Lock'
                     else
                        'Share Metadata Lock'
                     end,
                     'N',DATE_ADD(NOW(),INTERVAL-TIME SECOND),'{1}'
               FROM information_schema.processlist p
                WHERE id IN(SELECT trx_mysql_thread_id FROM information_schema.innodb_trx)
                   AND exists(select 1 from devops.t_metadata_lock_table m 
                              where m.db=p.db and instr(p.info,concat(' ',m.tname))>0)
           """.format(nowTime,nowTime)
       cr.execute("truncate table devops.t_metadata_lock_table")
       cr.execute(sql)
       cr.execute("select count(distinct status) from devops.t_metadata_lock where gather_time='{0}'".format(nowTime))
       rs2=cr.fetchone()
       if rs2[0]!=2:
         cr.execute("delete from devops.t_metadata_lock where gather_time='{0}'".format(nowTime))
       print("gather db1 metadata lock info:"+nowTime)
    config['db1'].commit()
    cr.close()

def get_html_contents(config,thead,tbody):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_html='''<html>
		<head>
		   <style type="text/css">
			   .xwtable {width: 120%;border-collapse: collapse;border: 1px solid #ccc;}
			   .xwtable thead td {font-size: 12px;color: #333333;
					      text-align: center;background: url(table_top.jpg) repeat-x top center;
				              border: 1px solid #ccc; font-weight:bold;}
			   .xwtable thead th {font-size: 12px;color: #333333;
				              text-align: center;background: url(table_top.jpg) repeat-x top center;
					      border: 1px solid #ccc; font-weight:bold;}
			   .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
			   .xwtable tbody tr.alt-row {background: #f2f7fc;}
			   .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
		   </style>
		</head>
		<body>
		  <h4>发送时间：'''+nowTime+'''</h4>
		  <h4>数据库IP：'''+config['db1_ip']+'''</h4>
		  <h4>数据端口：'''+config['db1_port']+'''</h4>
		  <h4>当前连接：'''+get_conn_info(config)+'''</h4>
		  <table class="xwtable">
			<thead>\n'''+thead+'\n</thead>\n'+'<tbody>\n'+tbody+'''\n</tbody>
		  </table>
		</body>
	    </html>
           '''.format(thead,tbody)
    return v_html

def get_html_contents_slave(config,thead,tbody):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_html='''<html>
		<head>
		   <style type="text/css">
			   .xwtable {width: 120%;border-collapse: collapse;border: 1px solid #ccc;}
			   .xwtable thead td {font-size: 12px;color: #333333;
					      text-align: center;background: url(table_top.jpg) repeat-x top center;
				              border: 1px solid #ccc; font-weight:bold;}
			   .xwtable thead th {font-size: 12px;color: #333333;
				              text-align: center;background: url(table_top.jpg) repeat-x top center;
					      border: 1px solid #ccc; font-weight:bold;}
			   .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
			   .xwtable tbody tr.alt-row {background: #f2f7fc;}
			   .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
		   </style>
		</head>
		<body>
		  <h4>发送时间：'''+nowTime+'''</h4>
		  <h4>数据库IP：'''+config['db2_ip']+'''</h4>
		  <h4>数据端口：'''+config['db2_port']+'''</h4>
		  <h4>当前连接：'''+get_conn_info(config)+'''</h4>
		  <table class="xwtable">
			<thead>\n'''+thead+'\n</thead>\n'+'<tbody>\n'+tbody+'''\n</tbody>
		  </table>
		</body>
	    </html>
           '''.format(thead,tbody)
    return v_html

def get_html_logon(config,p_error,p_times):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tbody ='''<tr><td width=10%><b>描述</b></td> <td width=40%>{0}</td></tr>
              <tr><td width=10%><b>数据库</b></td> <td width=40%>{1}</td></tr>
              <tr><td width=10%><b>用户名</b></td> <td width=40%>{2}</td></tr>
              <tr><td width=10%><b>错误代码</b></td> <td width=40%>{3}</td></tr> 
              <tr><td width=10%><b>错误消息</b></td> <td width=40%>{4}</td></tr>
              <tr><td width=10%><b>重试次数</b></td> <td width=40%>{5}</td></tr>
           '''.format(config['project_name'],config['db1_service'],config['db1_user'], p_error[0], p_error[1],str(p_times))

    v_html='''<html>
                <head>
                   <style type="text/css">
                       .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                       .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                      border: 1px solid #ccc; font-weight:bold;}
                       .xwtable thead th {font-size: 12px;color: #333333;
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                       .xwtable tbody tr.alt-row {background: #f2f7fc;}
                       .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                   </style>
                </head>
                <body>
                  <h4>发送时间：'''+nowTime+'''</h4>
                  <table class="xwtable">
                    <tbody>\n'''+tbody+'\n</tbody>\n'+'''
                  </table>
                </body>
                </html>
           '''.format(tbody)
    return v_html

def get_html_ms_sync(config,d_sync):
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    master  = config['db1_ip'] + ":" + config['db1_port'] + "/" + config['db1_service']
    slave   = config['db2_ip'] + ":" + config['db2_port'] + "/" + config['db2_service']
    v_tbody ='''<tr><td width=10%><b>主库信息</b></td> <td width=40%>{0}</td></tr>
               <tr><td><b>从库信息</b></td> <td>{1}</td></tr>
               <tr><td><b>主库时钟</b></td> <td>{2}</td></tr>                        
               <tr><td><b>从库时钟</b></td> <td>{3}</td></tr>
               <tr><td><b>主从延时</b></td> <td>{4}s</td></tr>  
           '''.format(master, slave,d_sync['master_clock'],d_sync['slave_clock'],str(d_sync['master_slave_relay']))

    v_content = '''<html>
                      <head>
                         <style type="text/css">  
                           .xwtable {width: 50%;border-collapse: collapse;border: 1px solid #ccc;}                  
                           .xwtable thead td {font-size: 12px;color: #333333;
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                      border: 1px solid #ccc; font-weight:bold;}  
                           .xwtable thead th {font-size: 12px;color: #333333;                                          
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                      border: 1px solid #ccc; font-weight:bold;}   
                           .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}             
                           .xwtable tbody tr.alt-row {background: #f2f7fc;}                 
                           .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}  
                         </style> 
                     </head>
                     <body>
                        <h4>发送时间：''' + nowTime + '''</h4>
                        <table class="xwtable">
                          <tbody>\n''' + v_tbody + '''\n</tbody>
                        </table>
                     </body>
                    </html>'''
    return v_tbody

def get_slow_records(config):
    cr = config['db1'].cursor()
    if config['mon_db'] == '*':
        p_sql = """Select  count(0)
                    From information_schema.processlist  
                    Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  and time>={0}  order by time desc
                """.format(config['slow_query_time'])
    else:
        p_sql = """Select count(0)
                   From information_schema.processlist  
                   Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                     and time>={0} and instr('{1}', db)>0 order by time desc
                 """.format(config['slow_query_time'], config['mon_db'])
    cr.execute(p_sql)    
    rs=cr.fetchall()
    n_slow_sql=len(rs)
    config['db1'].commit()
    cr.close()
    return n_slow_sql

def get_slow_records_slave(config):
    cr = config['db2'].cursor()
    if config['mon_db'] == '*':
        p_sql = """Select  count(0)
                    From information_schema.processlist  
                    Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  and time>={0}  order by time desc
                """.format(config['slow_query_time'])
    else:
        p_sql = """Select count(0)
                   From information_schema.processlist  
                   Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                     and time>={0} and instr('{1}', db)>0 order by time desc
                 """.format(config['slow_query_time'], config['mon_db'])
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_slow_sql=len(rs)
    config['db2'].commit()
    cr.close()
    return n_slow_sql


def get_block_records(config):
    p_sql = """SELECT distinct 
                      b.trx_id      AS '事务ID',
                      a.id          AS '线程ID',
                      a.host        AS '主机',
                      a.db          AS '数据库',
                      a.user        AS '用户',
                      IFNULL(e.block_trx_id,'') AS '阻塞事务ID',
                      b.trx_started AS '事务开始时间',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         a.time 
                      ELSE 
                        TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)  END AS '时长(s)',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         'WAITTING' 
                      ELSE 'BLOCKING'  END AS '事务状态',
                      a.state       AS '进程状态',
                      b.trx_isolation_level AS '隔离级别',
                      c.lock_table  AS '锁定表',
                      c.lock_mode   AS '锁模式',
                      c.lock_type   AS '锁类型',
                      b.trx_query   AS 'SQL语句'
                FROM information_schema.processlist a
		INNER JOIN information_schema.innodb_trx  b ON b.trx_mysql_thread_id=a.id
		INNER JOIN information_schema.INNODB_LOCKS c ON b.trx_id=c.lock_trx_id
		LEFT  JOIN (SELECT w.requesting_trx_id,
					GROUP_CONCAT(CONCAT(w.blocking_trx_id,'|') ORDER BY t.trx_started  SEPARATOR '') AS block_trx_id,
					GROUP_CONCAT(CONCAT('kill ',t.trx_mysql_thread_id,'; \n') ORDER BY t.trx_started  SEPARATOR '') AS kill_thread
	     		    FROM information_schema.innodb_lock_waits w,
				 information_schema.innodb_trx  t
			    WHERE w.blocking_trx_id=t.trx_id
			    GROUP BY w.requesting_trx_id) e ON b.trx_id=e.requesting_trx_id
                WHERE  CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN
                           a.time
                       ELSE
                           TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)
                       END >={0}     
	         ORDER BY b.trx_started
            """.format(config['trx_block_time'])
    cr = config['db1'].cursor()
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_records=len(rs)
    config['db1'].commit()
    cr.close()
    return n_records

def get_block_records_slave(config):
    p_sql = """SELECT distinct 
                      b.trx_id      AS '事务ID',
                      a.id          AS '线程ID',
                      a.host        AS '主机',
                      a.db          AS '数据库',
                      a.user        AS '用户',
                      IFNULL(e.block_trx_id,'') AS '阻塞事务ID',
                      b.trx_started AS '事务开始时间',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         a.time 
                      ELSE 
                        TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)  END AS '时长(s)',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         'WAITTING' 
                      ELSE 'BLOCKING'  END AS '事务状态',
                      a.state       AS '进程状态',
                      b.trx_isolation_level AS '隔离级别',
                      c.lock_table  AS '锁定表',
                      c.lock_mode   AS '锁模式',
                      c.lock_type   AS '锁类型',
                      b.trx_query   AS 'SQL语句'
                FROM information_schema.processlist a
		INNER JOIN information_schema.innodb_trx  b ON b.trx_mysql_thread_id=a.id
		INNER JOIN information_schema.INNODB_LOCKS c ON b.trx_id=c.lock_trx_id
		LEFT  JOIN (SELECT w.requesting_trx_id,
					GROUP_CONCAT(CONCAT(w.blocking_trx_id,'|') ORDER BY t.trx_started  SEPARATOR '') AS block_trx_id,
					GROUP_CONCAT(CONCAT('kill ',t.trx_mysql_thread_id,'; \n') ORDER BY t.trx_started  SEPARATOR '') AS kill_thread
	     		    FROM information_schema.innodb_lock_waits w,
				 information_schema.innodb_trx  t
			    WHERE w.blocking_trx_id=t.trx_id
			    GROUP BY w.requesting_trx_id) e ON b.trx_id=e.requesting_trx_id
                WHERE  CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN
                           a.time
                       ELSE
                           TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)
                       END >={0}     
	         ORDER BY b.trx_started
            """.format(config['trx_block_time'])
    cr = config['db2'].cursor()
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_records=len(rs)
    config['db2'].commit()
    cr.close()
    return n_records

def get_txn_records(config):
    p_sql = "SELECT count(0) from information_schema.innodb_trx"
    cr = config['db1'].cursor()
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_records=len(rs)
    config['db1'].commit()
    cr.close()
    return n_records

def get_txn_records_slave(config):
    p_sql = "SELECT count(0) from information_schema.innodb_trx"
    cr = config['db2'].cursor()
    cr.execute(p_sql)
    rs=cr.fetchall()
    n_records=len(rs)
    config['db2'].commit()
    cr.close()
    return n_records

def get_active_conns(config):
    cr = config['db1'].cursor()
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    config['db1'].commit()
    cr.close()
    return p_conn_active

def get_active_conns_slave(config):
    cr = config['db2'].cursor()
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    config['db2'].commit()
    cr.close()
    return p_conn_active

def get_total_conns(config):
    cr = config['db1'].cursor()
    p_conn_sql = "SELECT count(0) FROM information_schema.processlist"
    cr.execute(p_conn_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    config['db1'].commit()
    cr.close()
    #print("get_total_conns=",p_conn_total)
    return p_conn_total

def get_total_conns_slave(config):
    cr = config['db2'].cursor()
    p_conn_sql = "SELECT count(0) FROM information_schema.processlist"
    cr.execute(p_conn_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    config['db2'].commit()
    cr.close()
    return p_conn_total

def get_threshold(config):
    v_content="""
                  连接总会话:{0}<br>
                  活动会话数:{1}<br>
                  慢查询数量:{2}<br>
                  事务记录数:{3}<br>
                  锁等待数量:{4}
              """.format(config['total_connections'],config['active_connections'],config['slow_query_numbers'],config['max_trans_numbers'],config['lock_enqueue_numbers'])
    return v_content

def if_exceed_threshold(config):
    if (get_active_conns(config)  >= int(config['active_connections'])) or \
       (get_slow_records(config)  >= int(config['slow_query_numbers'])) or \
       (get_txn_records(config)   >= int(config['max_trans_numbers']))    or \
       (get_block_records(config) >= int(config['lock_enqueue_numbers'])) or \
       (get_total_conns(config)   >= int(config['total_connections'])):
       return True
    else:
       return False 

def get_echart_option_data(config,flag):
    sql=''
    v_begin,v_end=get_option_time_where()
    if flag=='TOTAL_CONNECTIONS':
       sql="SELECT connection_total,DATE_FORMAT(tjrq,'%h:%i') FROM t_db_monitor WHERE tjrq BETWEEN '{0}' AND '{1}'  and host='{2}' ORDER BY tjrq".format(v_begin,v_end,config['db1_ip'])
    elif flag=='ACTIVE_CONNECTIONS':
       sql="SELECT connection_active,DATE_FORMAT(tjrq,'%h:%i') FROM t_db_monitor WHERE tjrq BETWEEN '{0}' AND '{1}' and host='{2}' ORDER BY tjrq".format(v_begin,v_end,config['db1_ip'])
    else:
       sql=""
    print("sql=",sql) 
    cr = config['db1'].cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    v_data_x=''
    v_data_y=''
    for i in rs:
       v_data_x=v_data_x+'"'+str(i[1])+'",'
       v_data_y=v_data_y+str(i[0])+','
    config['db1'].commit()
    cr.close()
    return v_data_x[0:-1],v_data_y[0:-1]

def get_echart_option_data_slave(config,flag):
    sql=''
    v_begin,v_end=get_option_time_where()
    if flag == 'TOTAL_CONNECTIONS':
       sql = "SELECT connection_total,DATE_FORMAT(tjrq,'%h:%i') FROM t_db_monitor WHERE tjrq BETWEEN '{0}' AND '{1}'  and host='{2}' ORDER BY tjrq".format(v_begin, v_end, config['db2_ip'])
    elif flag == 'ACTIVE_CONNECTIONS':
       sql = "SELECT connection_active,DATE_FORMAT(tjrq,'%h:%i') FROM t_db_monitor WHERE tjrq BETWEEN '{0}' AND '{1}' and host='{2}' ORDER BY tjrq".format(v_begin, v_end, config['db2_ip'])
    else:
       sql=""
    print("sql=",sql)
    cr = config['db1'].cursor()
    cr.execute(sql)
    rs=cr.fetchall()
    v_data_x=''
    v_data_y=''
    for i in rs:
       v_data_x=v_data_x+'"'+str(i[1])+'",'
       v_data_y=v_data_y+str(i[0])+','
    config['db1'].commit()
    cr.close()
    return v_data_x[0:-1],v_data_y[0:-1]

def get_option_time_where():
    v_begin=(datetime.datetime.now()+datetime.timedelta(hours=-1)+datetime.timedelta(minutes=-1)).strftime('%Y-%m-%d %H:%M:%S')
    v_end=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return v_begin,v_end 
      
def get_option_time_range():
    v_begin=(datetime.datetime.now()+datetime.timedelta(hours=-1)+datetime.timedelta(minutes=-1)).strftime("%H:%M")
    v_end=datetime.datetime.now().strftime('%H:%M')
    return '{0}~{1}'.format(v_begin,v_end)
   
def process_option_echart(config):
    #initial variable
    v_plug_dir=config['plugin_dir']
    print('v_plug_dir=',v_plug_dir)
    v_files=[]
    v_cmd='{0}/bin/phantomjs'.format(v_plug_dir)
    v_convert_js='{0}/js/echarts-convert.js'.format(v_plug_dir)
    v_option_js='{0}/js/options_echart.js'.format(v_plug_dir)
    v_echart_template_js='{0}/js/options_echart_template.js'.format(v_plug_dir)

    #--------------------generate total connection-------------------------
    v_title='总连接数:'+get_option_time_range()
    v_title_x='Time'
    v_title_y='Total'
    v_data_x,v_data_y=get_echart_option_data(config,'TOTAL_CONNECTIONS')
    v_cp="\cp -rf {0} {1}".format(v_echart_template_js,v_option_js)
    os.system(v_cp)
    os.system("sed -i 's/@TITLE/{0}'/g    {1}".format(v_title,v_option_js))
    os.system("sed -i 's/@XAXIS/{0}'/g    {1}".format(v_title_x,v_option_js))
    os.system("sed -i 's/@YAXIS/{0}'/g    {1}".format(v_title_y,v_option_js))
    os.system("sed -i 's/@DATAX/{0}'/g    {1}".format(v_data_x,v_option_js))
    os.system("sed -i 's/@DATAY/{0}'/g    {1}".format(v_data_y,v_option_js))
    v_file='{0}/image/echarts{1}01.png'.format(v_plug_dir,datetime.datetime.now().strftime('%M'))
    v_exe="{0} {1} -infile {2} -outfile {3}".format(v_cmd,v_convert_js,v_option_js,v_file)
    os.system(v_exe)
    v_files.append(v_file)
 
    #-------------------generate active connection------------------------
    v_title='活动连接:'+get_option_time_range()
    v_title_x='Time'
    v_title_y='Active'
    v_data_x,v_data_y=get_echart_option_data(config,'ACTIVE_CONNECTIONS')
    v_cp="\cp -rf {0} {1}".format(v_echart_template_js,v_option_js)
    os.system(v_cp)
    os.system("sed -i 's/@TITLE/{0}'/g    {1}".format(v_title,v_option_js))
    os.system("sed -i 's/@XAXIS/{0}'/g    {1}".format(v_title_x,v_option_js))
    os.system("sed -i 's/@YAXIS/{0}'/g    {1}".format(v_title_y,v_option_js))
    os.system("sed -i 's/@DATAX/{0}'/g    {1}".format(v_data_x,v_option_js))
    os.system("sed -i 's/@DATAY/{0}'/g    {1}".format(v_data_y,v_option_js))
    v_file='{0}/image/echarts{1}02.png'.format(v_plug_dir,datetime.datetime.now().strftime('%M'))
    v_exe="{0} {1} -infile {2} -outfile {3}".format(v_cmd,v_convert_js,v_option_js,v_file)
    os.system(v_exe)
    v_files.append(v_file)
    return v_files

def process_option_echart_slave(config):
    # initial variable
    v_plug_dir = config['plugin_dir']
    print('v_plug_dir=', v_plug_dir)
    v_files = []
    v_cmd = '{0}/bin/phantomjs'.format(v_plug_dir)
    v_convert_js = '{0}/js/echarts-convert.js'.format(v_plug_dir)
    v_option_js = '{0}/js/options_echart.js'.format(v_plug_dir)
    v_echart_template_js = '{0}/js/options_echart_template.js'.format(v_plug_dir)

    # --------------------generate total connection-------------------------
    v_title = '总连接数:' + get_option_time_range()
    v_title_x = 'Time'
    v_title_y = 'Total'
    v_data_x, v_data_y = get_echart_option_data_slave(config, 'TOTAL_CONNECTIONS')
    v_cp = "\cp -rf {0} {1}".format(v_echart_template_js, v_option_js)
    os.system(v_cp)
    os.system("sed -i 's/@TITLE/{0}'/g    {1}".format(v_title, v_option_js))
    os.system("sed -i 's/@XAXIS/{0}'/g    {1}".format(v_title_x, v_option_js))
    os.system("sed -i 's/@YAXIS/{0}'/g    {1}".format(v_title_y, v_option_js))
    os.system("sed -i 's/@DATAX/{0}'/g    {1}".format(v_data_x, v_option_js))
    os.system("sed -i 's/@DATAY/{0}'/g    {1}".format(v_data_y, v_option_js))
    v_file = '{0}/image/echarts{1}01.png'.format(v_plug_dir, datetime.datetime.now().strftime('%M'))
    v_exe = "{0} {1} -infile {2} -outfile {3}".format(v_cmd, v_convert_js, v_option_js, v_file)
    os.system(v_exe)
    v_files.append(v_file)

    # -------------------generate active connection------------------------
    v_title = '活动连接:' + get_option_time_range()
    v_title_x = 'Time'
    v_title_y = 'Active'
    v_data_x, v_data_y = get_echart_option_data(config, 'ACTIVE_CONNECTIONS')
    v_cp = "\cp -rf {0} {1}".format(v_echart_template_js, v_option_js)
    os.system(v_cp)
    os.system("sed -i 's/@TITLE/{0}'/g    {1}".format(v_title, v_option_js))
    os.system("sed -i 's/@XAXIS/{0}'/g    {1}".format(v_title_x, v_option_js))
    os.system("sed -i 's/@YAXIS/{0}'/g    {1}".format(v_title_y, v_option_js))
    os.system("sed -i 's/@DATAX/{0}'/g    {1}".format(v_data_x, v_option_js))
    os.system("sed -i 's/@DATAY/{0}'/g    {1}".format(v_data_y, v_option_js))
    v_file = '{0}/image/echarts{1}02.png'.format(v_plug_dir, datetime.datetime.now().strftime('%M'))
    v_exe = "{0} {1} -infile {2} -outfile {3}".format(v_cmd, v_convert_js, v_option_js, v_file)
    os.system(v_exe)
    v_files.append(v_file)
    return v_files

def get_db_health(config):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_db_string=config['db1_ip'] + ':' + config['db1_port'] + '/' + config['mon_db']
    v_active_conns="<font color='red'>"+str(get_active_conns(config))+"</font>"\
                     if get_active_conns(config)>=int(config['active_connections']) else str(get_active_conns(config))
    v_total_conns="<font color='red'>"+str(get_total_conns(config))+"</font>"\
                     if get_total_conns(config)>=int(config['total_connections']) else str(get_total_conns(config)) 
    v_slow_records="<font color='red'>"+str(get_slow_records(config))+"</font>"\
                     if get_slow_records(config)>=int(config['slow_query_numbers']) else str(get_slow_records(config))
    v_trx_records ="<font color='red'>"+str(get_txn_records(config))+"</font>"\
                     if get_txn_records(config)>=int(config['max_trans_numbers'])  else str(get_slow_records(config)) 
    v_block_records="<font color='red'>"+str(get_block_records(config))+"</font>"\
                     if get_block_records(config)>=int(config['lock_enqueue_numbers']) else str(get_block_records(config)) 
    
    v_tbody ='''<tr><td width=10%><b>环境描述</b></td> <td width=40%>{0}</td></tr>
	        <tr><td width=10%><b>数据库</b></td> <td width=40%>{1}</td></tr>
   	        <tr><td width=10%><b>用户名</b></td> <td width=40%>{2}</td></tr>
                <tr><td width=10%><b>连接数总数</b></td> <td width=40%>{3}</td></tr>
                <tr><td width=10%><b>活动连接数</b></td> <td width=40%>{4}</td></tr>
                <tr><td width=10%><b>慢查询数量</b></td> <td width=40%>{5}</td></tr>
                <tr><td width=10%><b>事务记录数</b></td> <td width=40%>{6}</td></tr>
                <tr><td width=10%><b>锁等待数量</b></td> <td width=40%>{7}</td></tr>
                <tr><td width=10%><b>预警阀值数</b></td> <td width=40%>{8}</td></tr>
            '''.format(config['project_name'],v_db_string,config['db1_user'],v_total_conns,v_active_conns,\
                       v_slow_records,v_trx_records,v_block_records,get_threshold(config))

    v_html='''<html>
                  <head>
                       <style type="text/css">
                                    p {width:40%;}
                                    body {border :1px;}
                       .xwtable {width: 40%;border-collapse: collapse;border: 1px solid #ccc;padding-left:50px;}
                       .xwtable thead td {font-size: 12px;color: #333333;
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable thead th {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                       .xwtable tbody tr.alt-row {background: #f2f7fc;}
                       .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                                   .image{width: 800px;height:500px;}
                       </style>
                 </head>
	         <body>
		    <p align="center"><b>'''+config['db_health_title']+'''</b></p>
		    <table class="xwtable">
	               	<tbody>\n'''+v_tbody+'\n</tbody>\n'+'''
		    </table>
                    <br>发送时间：'''+nowTime+'''
	         </body>
            </html>
	  '''
    v_files=process_option_echart(config)
    print("v_files=",v_files)
    return v_html,v_files

def get_db_health_slave(config):
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_db_string = config['db2_ip'] + ':' + config['db2_port'] + '/' + config['mon_db']
    v_active_conns = "<font color='red'>" + str(get_active_conns_slave(config)) + "</font>" \
        if get_active_conns_slave(config) >= int(config['active_connections']) else str(get_active_conns_slave(config))
    v_total_conns = "<font color='red'>" + str(get_total_conns_slave(config)) + "</font>" \
        if get_total_conns_slave(config) >= int(config['total_connections']) else str(get_total_conns_slave(config))
    v_slow_records = "<font color='red'>" + str(get_slow_records_slave(config)) + "</font>" \
        if get_slow_records_slave(config) >= int(config['slow_query_numbers']) else str(get_slow_records_slave(config))
    v_trx_records = "<font color='red'>" + str(get_txn_records_slave(config)) + "</font>" \
        if get_txn_records_slave(config) >= int(config['max_trans_numbers']) else str(get_txn_records_slave(config))
    v_block_records = "<font color='red'>" + str(get_block_records_slave(config)) + "</font>" \
        if get_block_records_slave(config) >= int(config['lock_enqueue_numbers']) else str(get_block_records_slave(config))

    v_tbody = '''<tr><td width=10%><b>环境描述</b></td> <td width=40%>{0}</td></tr>
	        <tr><td width=10%><b>数据库</b></td> <td width=40%>{1}</td></tr>
   	        <tr><td width=10%><b>用户名</b></td> <td width=40%>{2}</td></tr>
                <tr><td width=10%><b>连接数总数</b></td> <td width=40%>{3}</td></tr>
                <tr><td width=10%><b>活动连接数</b></td> <td width=40%>{4}</td></tr>
                <tr><td width=10%><b>慢查询数量</b></td> <td width=40%>{5}</td></tr>
                <tr><td width=10%><b>事务记录数</b></td> <td width=40%>{6}</td></tr>
                <tr><td width=10%><b>锁等待数量</b></td> <td width=40%>{7}</td></tr>
                <tr><td width=10%><b>预警阀值数</b></td> <td width=40%>{8}</td></tr>
            '''.format(config['project_name']+'-[SLAVE]', v_db_string, config['db1_user'], v_total_conns, v_active_conns, \
                       v_slow_records, v_trx_records, v_block_records, get_threshold(config))

    v_html = '''<html>
                  <head>
                       <style type="text/css">
                                    p {width:40%;}
                                    body {border :1px;}
                       .xwtable {width: 40%;border-collapse: collapse;border: 1px solid #ccc;padding-left:50px;}
                       .xwtable thead td {font-size: 12px;color: #333333;
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable thead th {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                       .xwtable tbody tr.alt-row {background: #f2f7fc;}
                       .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                                   .image{width: 800px;height:500px;}
                   </style>
                 </head>
	         <body>
		    <p align="center"><b>''' + config['db_health_title_slave'] + '''</b></p>
		    <table class="xwtable">
	               	<tbody>\n''' + v_tbody + '\n</tbody>\n' + '''
		    </table>
                    <br>发送时间：''' + nowTime + '''
	         </body>
            </html>
	  '''
    v_files = process_option_echart_slave(config)
    print("v_files=", v_files)
    return v_html, v_files


def get_tables_from_index_schema(config):
    cr = config['db1'].cursor()
    sql="""SELECT table_name FROM information_schema.tables
           WHERE table_schema='{0}' AND table_type='BASE TABLE' ORDER BY table_name
        """.format(config['index_schema'])
    cr.execute(sql)
    rs=cr.fetchall()
    v_tables=''
    for i in rs:
      v_tables=v_tables+i[0]+','
    config['db1'].commit()
    cr.close()
    return v_tables[0:-1]

def gather_indexes(fname):
    config=get_mon_conf_alone(fname)
    if not (config['index_method']=="normal" or config['index_method']=="pt"):
       print('please check config file [REBUILD_INDEX],parameter index_method only value:pt or normal!')
       sys.exit(0)

    check_logon_alone(config)
    cr = config['db1'].cursor()
    if config['index_tables']=='*':
       tabs=get_tables_from_index_schema(config).split(',')
    else:
       tabs=config['index_tables'].split(',')
   
    for k in range(len(tabs)):
       v_sql_index="SHOW INDEXES FROM {0}.{1}".format(config['index_schema'],tabs[k])
       #v_sql_del="delete from devops.t_db_indexes where schema_name='{0}' and table_name='{1}'".format(config['index_schema'],tabs[k])
       #cr.execute(v_sql_del)
       cr.execute(v_sql_index)
       ind_rs=cr.fetchall()
       for j in ind_rs:
          v_sql_ins="""insert into devops.t_db_indexes(schema_name,table_name,non_unique,key_name,seq_in_index,column_name,is_null,index_type,created)
                        values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')""".format(config['index_schema'],j[0],j[1],j[2],j[3],j[4],j[9],j[10],now())
          cr.execute(v_sql_ins)
       print("gathering index definition {0}.{1}...ok!".format(config['index_schema'],tabs[k]))
    config['db1'].commit()
    cr.close()
    return config

def rebuild_index_normal(config):
    file_handle = open(config['index_log'],'w')
    cr = config['db1'].cursor()
    if config['index_tables']=='*':
       tabs=get_tables_from_index_schema(config).split(',')
    else:
       tabs=config['index_tables'].split(',')

    for tab in tabs:
       v_re_index_sql="""SELECT
                              CASE WHEN index_type='FULLTEXT' THEN
                            	CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,', add ',index_type,' index ',key_name,'(',index_columns,')')	 
                              ELSE
                            	CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,', add index ',key_name,'(',index_columns,') using ',index_type) END AS  statement
                           FROM devops.v_index_gather
                          WHERE schema_name='{0}' and table_name='{1}'  AND key_name!='PRIMARY'
                          ORDER BY schema_name,table_name,key_name""".format(config['index_schema'],tab)
       cr.execute(v_re_index_sql)
       rs=cr.fetchall()
       for i in rs:
         cr.execute(i[0])
         file_handle.write(i[0]+'...ok!\n')
         print("\033[0;31;40mTable:\033[0m{0}.{1} rebuilding index success! [mode=normal]".format(config['index_schema'],tab))
    config['db1'].commit()
    file_handle.close()
    cr.close

def rebuild_index_pt(config):
    os.system('>{0}'.format(config['index_log']))
    cr = config['db1'].cursor()
    if os.system('which pt-online-schema-change')!=0:
       print("\033[0;31;40mWarning:Not Found pt-online-schema-change tools!!!\033[0m")

    print("\033[0;31;40mWarning:Please ensure database have enough free space!!!\033[0m")
    if config['index_tables']=='*':
       tabs=get_tables_from_index_schema(config).split(',')
    else:
       tabs=config['index_tables'].split(',')

    for tab in tabs:
       v_re_index_sql="""SELECT
                           CASE WHEN index_type='FULLTEXT' THEN
                              CONCAT('pt-online-schema-change -u {0} -p {1} -h {2} --port {3} --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --critical-load="Threads_running=200" --alter "drop index ',key_name,', add fulltext index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name,' &>>{4}')
                           ELSE
                              CONCAT('pt-online-schema-change -u {5} -p {6} -h {7} --port {8} --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --critical-load="Threads_running=200" --alter "drop index ',key_name,', add index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name,' &>>{9}') END  AS cre_index
                           FROM devops.v_index_gather
                          WHERE schema_name='{10}' and table_name='{11}'  AND key_name!='PRIMARY'
                          ORDER BY schema_name,table_name,key_name
                       """.format(config['db1_user'],config['db1_pass'],config['db1_ip'],config['db1_port'],config['index_log'],\
                                  config['db1_user'],config['db1_pass'],config['db1_ip'],config['db1_port'],config['index_log'],\
                                  config['index_schema'],tab)
       cr.execute(v_re_index_sql)
       rs=cr.fetchall()
       for i in rs:
         os.system(i[0])
         print("\033[0;31;40mTable:\033[0m{0}.{1} rebuild index success!y [mode=pt]".format(config['index_schema'],tab))
    config['db1'].commit()
    cr.close()

def rebuild_indexes(fname):
    config=get_mon_conf_alone(fname)
    if not (config['index_method']=="normal" or config['index_method']=="pt"):
       print('please check config file [REBUILD_INDEX],parameter "index_method" only value:pt or normal!')
       sys.exit(0)
    check_logon_alone(config)
    if config['index_method']=="normal":
       rebuild_index_normal(config)
    elif config['index_method']=="pt":
       rebuild_index_pt(config)
    else:
       pass

def get_conf_binlog(fname):
    config=configparser.ConfigParser()
    config.read(fname,encoding="utf-8-sig")
    binlog_server         = config.get("BINLOG","server")
    binlog_file           = config.get("BINLOG","binlog_file")
    db_ip                 = binlog_server.split(':')[0]
    db_port               = binlog_server.split(':')[1]
    db_service            = binlog_server.split(':')[2]
    db_user               = binlog_server.split(':')[3]
    db_pass               = binlog_server.split(':')[4]
    config = {}
    config['db_ip']       = db_ip
    config['db_port']     = db_port
    config['db_service']  = db_service
    config['db_user']     = db_user
    config['db_pass']     = db_pass
    config['db_string']   = db_ip+':'+db_port+'/'+db_service
    config['binlog_file'] = binlog_file
    config['db']           = get_conn(db_ip,db_port ,db_service,db_user,db_pass)
    return config

def get_exp_query(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    return lines[0:-1]

def get_conf_exp(fname):
    t_cfg = {}
    config=configparser.ConfigParser()
    config.read(fname,encoding="utf-8-sig")
    server                = config.get("EXPORT","server")
    t_cfg['db_ip']       = server.split(':')[0]
    t_cfg['db_port']     = server.split(':')[1]
    t_cfg['db_service']  = server.split(':')[2]
    t_cfg['db_user']     = server.split(':')[3]
    t_cfg['db_pass']     = server.split(':')[4]
    t_cfg['db_string']   = t_cfg['db_ip']+':'+t_cfg['db_port']+'/'+t_cfg['db_service']
    t_cfg['database']    = config.get("EXPORT","database")
    t_cfg['exp_type']    = config.get("EXPORT", "exp_type")
    t_cfg['exp_query']   = config.get("EXPORT", "exp_query")
    t_cfg['db']           = get_conn(t_cfg['db_ip'], t_cfg['db_port'], t_cfg['db_service'], t_cfg['db_user'], t_cfg['db_pass'])
    return t_cfg

def format_sql(v_sql):
    v_statement=v_sql
    if "\\\\" in v_statement:
         v_statement=v_statement.replace("\\\\","!@#$").replace("\\'","'").replace("'","\\'").replace("!@#$","\\\\")
    else:
         v_statement=v_statement.replace("\\'","'").replace("\\","\\\\").replace("'","\\'")
    return v_statement

def parse_binlog(fname):
    config=get_conf_binlog(fname)
    print('MySQL binlog parameter...')
    output_parameter(config)
    cr = config['db'].cursor()
    for f in config['binlog_file'].split(','):
        print("Parseing binlog '{0}' please wait...".format(f))
        sql="""show binlog events in '{0}'""".format(f)
        print(sql)
        cr.execute(sql)
        rs=cr.fetchall()
        i_counter=0
        i_counter2=0
        v_db=''
        for i in rs:
           if i[2]=='Query' and ('create' in i[5].lower() or 'alter' in i[5].lower() \
                                 or 'drop' in i[5].lower() or 'truncate' in i[5].lower()) and 't_db_binlog' not in i[5]:           
              v_db=i[5].split(';')[0].split(' ')[1].replace('`','')
              v_statement=i[5].replace("\\'","'").replace("'","\\'").split(';')[1][1:]
              v_sql_ins="""insert into devops.t_db_binlog(log_name,begin_pos,event_type,server_id,end_pos,statement,db) values('{0}','{1}','{2}','{3}','{4}','{5}','{6}')
                        """.format(i[0],i[1],i[2],i[3],i[4],v_statement,v_db)
              try:
                cr.execute(v_sql_ins)
                i_counter=i_counter+1
                if i_counter%100==0:
                  config['db'].commit()
              except:
                print(traceback.format_exc())
           
           if i[2]=='Rows_query' and 't_db_binlog' not in i[5]:
              v_sql_ins=''
              v_statement=i[5][2:]
              if "\\\\" in v_statement:   
                 v_statement=v_statement.replace("\\\\","!@#$").replace("\\'","'").replace("'","\\'").replace("!@#$","\\\\")
              else:
                 v_statement=v_statement.replace("\\'","'").replace("\\","\\\\").replace("'","\\'")

              v_sql_ins="""insert into devops.t_db_binlog(log_name,begin_pos,event_type,server_id,end_pos,statement,db) values('{0}','{1}','{2}','{3}','{4}','{5}','{6}')
                        """.format(i[0],i[1],i[2],i[3],i[4],v_statement,v_db)
              try:
                cr.execute(v_sql_ins)
                i_counter=i_counter+1
                if i_counter%100==0:
                  config['db'].commit()
              except:
                 print(traceback.format_exc())
        
           i_counter2=i_counter2+1
           if i_counter2%10000==0:
              print("\rTotal rec:{0},Process rec:{1}({2}),Complete:{3}%".format(len(rs),i_counter2,i_counter,round(i_counter2/len(rs)*100,2)),end='')

        print("\rTotal rec:{0},Process rec:{1}({2}),Complete:{3}%".format(len(rs),i_counter2,i_counter,round(i_counter2/len(rs)*100,2)),end='')
        config['db'].commit()
        print("\nbinlog '{0}' parse to table:'{1}' success!".format(f,'devops.t_db_binlog'))
    cr.close()

def main():
    #init variable 
    mode=""
    config=""
    debug=False
    warnings.filterwarnings("ignore")
    
    #get parameter from console    
    for p in range(len(sys.argv)): 
       if sys.argv[p] == "-conf":        
          config=sys.argv[p+1]
       elif sys.argv[p] == "-mode":
          mode=sys.argv[p+1]
       elif sys.argv[p] == "-debug":
          debug=True

    #process
    if mode == "alone":
       monitor_alone(config)	
    elif mode == "cluster":
       monitor_cluster(config)
    elif mode == "rebuild_indexes":
       rebuild_indexes(config)
    elif mode == "gather_indexes":
       gather_indexes(config)
    elif mode == "transfer":
       transfer(config,debug)
    elif mode == "binlog":
       parse_binlog(config)
    elif mode == "exp":
       exp(config)
    else:
       pass

if __name__ == "__main__":       
   main()

