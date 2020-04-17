##############################################################
# Memo:Use Backup_mysql Script Backup Localhost All DB 
# Version:2.0
##############################################################
source /etc/profile

#!/bin/bash
####################Basic parameters##########################
DATE=`date +%Y%m%d-%H`
DAY=`date +%Y%m%d`
YEAR=`date +%Y`
DAYS=7

####################Environment Parameters####################
USER='App_User'
PASSWD='winchannel2017'
DATA_BASE='/opt/db_backup/mysql_bak/dump'

DATA_DIR=${DATA_BASE}/${DAY}
mkdir -p ${DATA_DIR}
MYSQLDUMP='/usr/local/bin/mydumper'
MYSQL='/usr/bin/mysql'

####################Send Mail Parameters####################
db_ip='192.168.1.48'
db_port='3306'
send_time=$(date "+%Y-%m-%d %H:%M:%S")
thead="<th width=15%>数据库名</th>
       <th width=12%>开始时间</th>
       <th width=12%>完成时间</th>
       <th width=5%>备份耗时(s)</th>
       <th width=5%>压缩耗时(s)</th>
       <th width=20%>文件名</th>
       <th width=5%>文件大小</th>
       <th width=5%>完成情况</th>
       <th width=21%>错误消息</th>"
tbody=""
mail_title="MySQL开发数据库备份情况"
mail_status="0"

#kill active session
#for sess in `echo "SELECT id 
#                   FROM information_schema.processlist 
#                   WHERE db not IN('repl','event_scheduler','information_schema',
#                                    'performance_schema','test','sys','mysql')" | mysql -u${USER} -p${PASSWD} --skip-column-names`
#do
#   echo "Kill Session  ${sess}..."
#   ${MYSQL} -u${USER} -p${PASSWD} -e "kill ${sess}" &>/dev/null
#done

#mdumper backup
for db in `echo "SELECT schema_name 
                 FROM information_schema.schemata 
                 WHERE schema_name NOT IN ('information_schema','performance_schema','test','sys','mysql','sfa_yihaijiali_fenxiao')" | mysql -u${USER} -p${PASSWD} --skip-column-names`
do
   echo "Performing backup database ${db}..."
   start_time=$(date "+%Y-%m-%d %H:%M:%S")
   file_name=${DATA_DIR}/${db}_${DATE}.sql.gz
   file_name_short=${db}_${DATE}.sql.gz
   >error.log
   error=""
   ${MYSQLDUMP} -u "${USER}" -p "${PASSWD}" --kill-long-queries -R -E -B "${db}" -k -c -t 4 -o "${DATA_DIR}/${db}_${DATE}" &>error.log 
   if [ $? -eq 0 ]; then
      status="success"
   else
      status="failure"
      mail_status="1"
      sed -i s/"mysqldump: \[Warning\] Using a password on the command line interface can be insecure."//g error.log
      sed -i s/"mysqldump:"/"<br>mysqldump:"/g error.log
      error=`more error.log` 
   fi
   end_time_backup=$(date "+%Y-%m-%d %H:%M:%S")
   tar -czPf "${file_name}" "${DATA_DIR}/${db}_${DATE}"
   end_time_gzip=$(date "+%Y-%m-%d %H:%M:%S")
   file_size=`ls -lh "${file_name}" | awk '{print $5}'`

   begin_time_backup=`date -d "${start_time}" +%s`
   end_time_backup=`date -d "${end_time_backup}" +%s`   
   end_time_gzip_s=`date -d "${end_time_gzip}" +%s`

   consume_backup=`expr "${end_time_backup}" - "${begin_time_backup}"`
   consume_gzip=`expr "${end_time_gzip_s}" - "${end_time_backup}"`
   echo "rm directory:${DATA_DIR}/${db}_${DATE}"
   rm -rf "${DATA_DIR}/${db}_${DATE}"

   echo "start_time:${start_time}, end_time:${end_time}, status=${status},consume_backup=${consume_backup}s,consume_gzip=${consume_gzip}s, file_size=${file_size}"
   row="<tr>
            <td>${db}</td>
            <td>${start_time}</td>
            <td>${end_time_gzip}</td>
            <td>${consume_backup}</td>
            <td>${consume_gzip}</td> 
            <td>${file_name_short}</td>
            <td>${file_size}</td>
            <td>${status}</td>
            <td color=\"red\">${error}</td>
       </tr>"
   tbody=${tbody}${row}   
done
complete_time=$(date "+%Y-%m-%d %H:%M:%S")
complete_time_s=`date -d "${complete_time}" +%s`
send_time_s=`date -d "${send_time}" +%s`
total_consume=`expr "${complete_time_s}" - "${send_time_s}"`

echo >mail_content.html
echo '<html>' >>mail_content.html
echo '  <head>' >>mail_content.html
echo '    <style type="text/css">' >>mail_content.html
echo '      .xwtable {width: 120%;border-collapse: collapse;border: 1px solid #000;}' >>mail_content.html
echo '      .xwtable thead th {font-size: 12px;color: #333333;' >>mail_content.html                                          
echo '                         text-align: center;background: url(table_top.jpg) repeat-x top center;' >>mail_content.html
echo '                         border: 1px solid #ccc; font-weight:bold;}' >>mail_content.html
echo '      .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;} ' >>mail_content.html             
echo '      .xwtable tbody tr.alt-row {background: #f2f7fc;}' >>mail_content.html                 
echo '      .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}' >>mail_content.html      
echo '      .xwtable thead td {font-size: 12px;color: #333333; ' >>mail_content.html
echo '                         text-align: center;background: url(table_top.jpg) repeat-x top center; ' >>mail_content.html
echo '                         border: 1px solid #ccc; font-weight:bold;} ' >>mail_content.html
echo '    </style>' >>mail_content.html
echo '  </head>' >>mail_content.html
echo '  <body>' >>mail_content.html
echo '     <h4>发送时间  :' ${send_time}  '</h4>' >>mail_content.html
echo '     <h4>数据库地址:' ${db_ip}      '</h4>' >>mail_content.html
echo '     <h4>数据库端口:' ${db_port}    '</h4>' >>mail_content.html
echo '     <h4>备份路径  :' ${DATA_BASE}  '</h4>' >>mail_content.html
echo '     <table class="xwtable"> ' >>mail_content.html
echo '        <thead>    ' >>mail_content.html
echo '          '${thead}  >>mail_content.html
echo '        </thead>    '>>mail_content.html
echo '        <tbody>     '>>mail_content.html
echo '          '${tbody}  >>mail_content.html
echo '        </tbody>    '>>mail_content.html
echo '     </table>       '>>mail_content.html
echo '    <h4>总计耗时  :' ${total_consume}'s </h4>'>>mail_content.html 
echo '    <body> ' >>mail_content.html
echo '  </html>  ' >>mail_content.html

#mail_content=`cat mail_content.html 2>1& >/dev/null`
mail_content=`cat mail_content.html`

#backup cnf file
cp /etc/my.cnf ${DATA_DIR}/my.cnf-${DATE}.cnf

#delete expired backup
find ${DATA_BASE} -name "${YEAR}*" -type d -mtime +${DAYS} -exec rm -rf {} \;

if [ "${mail_status}" -eq "1" ]; then
  mail_title=${mail_title}"*异常"
fi

#send email from python script
/home/dba/3306/sendmail.py "${mail_title}" "${mail_content}"

#delete temp file
rm gmon.out
rm mail_content.html
rm error.log

