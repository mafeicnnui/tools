rem ##############################################################
rem # Memo:Use Backup_mysql Script Backup Localhost All DB
rem # Version:3.0
rem ##############################################################
@echo off
rem set vDATE=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%
if "%time:~0,2%"==" 9" (
  set vDATE=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~1,1%
) else (
  set vDATE=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%
) 

set vDAY=%date:~0,4%%date:~5,2%%date:~8,2%
set vYEAR=%date:~0,4%
set vDAYS=7

set USER=puppet
set PASSWD=Puppet@123
set DATA_BASE=E:\mysql\backup
set DATA_DIR=%DATA_BASE%\%vDAY%
set MYSQLDUMP=E:\mysql\bin\mysqldump
set MYSQL=E:\mysql\bin\mysql
mkdir %DATA_DIR%

echo %vDATE%,%vDAY%
echo %USER%,%PASSWD%
echo %DATA_BASE%
echo %DATA_DIR%
echo %MYSQLDUMP%
echo %MYSQL%

set db_ip=172.22.22.235
set db_port=8635
set send_time=%date:~0,4%-%date:~5,2%-%date:~8,2% %time:~0,2%:%time:~3,2%:%time:~6,2%
set send_time_s=%time:~,-3%

set tbody=
set mail_title=恒天然DMS/MySQL数据库备份情况
set mail_status=0
set status=success
echo SELECT schema_name FROM information_schema.schemata WHERE schema_name not in ('information_schema','performance_schema','test','sys','mysql') | mysql -upuppet -pPuppet@123 -h172.22.22.235 --port 8635 --skip-column-names >db.txt
echo ''>error.log
@echo off&setlocal enabledelayedexpansion
for /f %%i in (db.txt) do (
   echo Performing backup database %%i...
   set start_time=%date:~0,4%-%date:~5,2%-%date:~8,2% %time:~0,2%:%time:~3,2%:%time:~6,2%
   set begin_time_backup_s=%time:~,-3%
   set file_name_no_data=%DATA_DIR%\%%i_%vDATE%_no_data.sql
   set file_name_data=%DATA_DIR%\%%i_%vDATE%_data.sql
   set file_name_short_no_data=%%i_%vDATE%_no_data.sql.zip
   set file_name_short_data=%%i_%vDATE%_data.sql.gz
   echo ''>error.log
   set error=''   
   echo start_time=!start_time!
   echo file_name_no_data=!file_name_no_data!
   echo file_name_data=!file_name_data!
   echo file_name_short_no_data=!file_name_short_no_data!
   echo file_name_short_data=!file_name_short_data!
   
   !MYSQLDUMP! -u!USER! -p!PASSWD! -h!db_ip! --port !db_port! --single-transaction --routines --no-data  -f %%i -r !file_name_no_data!
   if !ERRORLEVEL! EQU 0 (
       !MYSQLDUMP! -u!USER! -p!PASSWD! -h!db_ip! --port !db_port! --single-transaction --routines --no-create-info  -f %%i -r !file_name_data!
	   set end_time_backup=%date:~0,4%-%date:~5,2%-%date:~8,2% %time:~0,2%:%time:~3,2%:%time:~6,2%
	   set end_time_backup_s=%time:~,-3%
	   if !ERRORLEVEL! EQU 0 (
	        set status=success
			set mail_status=1
			makecab !file_name_no_data! !file_name_no_data!.zip 
            makecab !file_name_data! !file_name_data!.zip
			del !file_name_no_data! 
			del !file_name_data!			
			set end_time_gzip=%date:~0,4%-%date:~5,2%-%date:~8,2% %time:~0,2%:%time:~3,2%:%time:~6,2%
			set end_time_gzip_s=%time:~,-3%
            echo !file_name_no_data!.zip
			echo !file_name_data!.zip			
            for /f %%a in ('dir/s/b !file_name_no_data!.zip') do set file_size_no_data=%%~za
			for /f %%a in ('dir/s/b !file_name_data!.zip') do set file_size_data=%%~za				
            
	   ) else (
	        set status=failure
            set mail_status=1
			!MYSQLDUMP! -u!USER! -p!PASSWD! -h!db_ip! --port !db_port! --single-transaction --routines --no-data  -f %%iaa -r !file_name_no_data! >>error.log 2>&1
	   )         
   ) else (       
	   set status=failure
       set mail_status=1
	   !MYSQLDUMP! -u!USER! -p!PASSWD! -h!db_ip! --port !db_port! --single-transaction --routines --no-data  -f %%iaa -r !file_name_no_data! >>error.log 2>&1
   )
   
   echo output=!status!,!mail_status!   
   echo end_time_backup=!end_time_backup!
   echo end_time_gzip=!end_time_gzip!
   echo !file_size_no_data!
   echo !file_size_data!
   
   echo begin_time_backup_s:!begin_time_backup_s!
   echo end_time_backup_s:!end_time_backup_s!
   echo end_time_gzip_s:!end_time_gzip_s!
  
   set/a s1=!begin_time_backup_s:~0,2!*3600+!begin_time_backup_s:~3,2!*60+!begin_time_backup_s:~6,2!
   set/a s2=!end_time_backup_s:~0,2!*3600+!end_time_backup_s:~3,2!*60+!end_time_backup_s:~6,2!  
   set/a consume_backup=!s2!-!s1!
   
   set/a s1=!end_time_backup_s:~0,2!*3600+!end_time_backup_s:~3,2!*60+!end_time_backup_s:~6,2!
   set/a s2=!end_time_gzip_s:~0,2!*3600+!end_time_gzip_s:~3,2!*60+!end_time_gzip_s:~6,2!  
   set/a consume_gzip=!s2!-!s1!
   
   echo consume_backup:!begin_time_backup_s!,!end_time_backup!,!consume_backup!
   echo consume_gzip:!end_time_backup!,!end_time_gzip!,!consume_gzip!   
   
   if "%mail_status%"=="0" (
      set  row=^<tr^>^<td^>%%i^</td^>^<td^>!start_time!^</td^>^<td^>!end_time_gzip!^</td^>^<td^>!consume_backup!^</td^>^<td^>!consume_gzip!^</td^>^<td^>!file_name_short_no_data!^<br^>!file_name_short_data!^</td^>^<td^>!file_size_no_data!^<br^>!file_size_data!^</td^>^<td^>!status!^</td^>^<td color=\"red\"^>^</td^>^</tr^>
   ) else (
      set  row=^<tr^>^<td^>%%i^</td^>^<td^>!start_time!^</td^>^<td^>!end_time_gzip!^</td^>^<td^>!consume_backup!^</td^>^<td^>!consume_gzip!^</td^>^<td^>!file_name_short_no_data!^<br^>!file_name_short_data!^</td^>^<td^>!file_size_no_data!^<br^>!file_size_data!^</td^>^<td^>!status!^</td^>^<td color=\"red\"^>error^</td^>^</tr^>
   )
   set tbody=!tbody!^!row!
 )
echo !tbody! 

set complete_time_s=%time:~,-3%
set/a s1=!send_time_s:~0,2!*3600+!send_time_s:~3,2!*60+!send_time_s:~6,2!
set/a s2=!complete_time_s:~0,2!*3600+!complete_time_s:~3,2!*60+!complete_time_s:~6,2!
set/a total_consume=!s2!-!s1!
echo !send_time_s!,!complete_time_s!,!total_consume! 

echo >mail_content.html
echo ^<html^> >>mail_content.html
echo   ^<head^> >>mail_content.html
echo     ^<style type="text/css"^> >>mail_content.html
echo        .xwtable {width:120%%^;border-collapse:collapse^;border: 1px solid #000^;} >>mail_content.html
echo        .xwtable thead th {font-size: 12px^;color: #333333^; >>mail_content.html
echo                           text-align: center^;background: url(table_top.jpg) repeat-x top center^; >>mail_content.html
echo                           border: 1px solid #ccc^; font-weight:bold^;} >>mail_content.html
echo        .xwtable tbody tr {background: #fff^;font-size: 12px^;color: #666666^;}  >>mail_content.html
echo        .xwtable tbody tr.alt-row {background: #f2f7fc^;} >>mail_content.html
echo        .xwtable td{line-height:20px^;text-align:left^;padding:4px 10px 3px 10px^;height:18px^;border:1px solid #ccc^;} >>mail_content.html
echo        .xwtable thead td {font-size: 12px^;color: #333333^;  >>mail_content.html
echo                           text-align: center^;background: url(table_top.jpg) repeat-x top center^;  >>mail_content.html
echo                           border: 1px solid #ccc^; font-weight:bold^;}  >>mail_content.html
echo     ^</style^> >>mail_content.html
echo   ^</head^> >>mail_content.html
echo   ^<body^> >>mail_content.html
echo       ^<h4^>发送时间  :%send_time%   ^</h4^> >>mail_content.html
echo       ^<h4^>数据库地址:%db_ip%      ^</h4^> >>mail_content.html
echo       ^<h4^>数据库端口:%db_port%    ^</h4^> >>mail_content.html
echo       ^<h4^>备份路径  :%DATA_BASE%  ^</h4^> >>mail_content.html
echo       ^<table class="xwtable"^>  >>mail_content.html
echo          ^<thead^>    >>mail_content.html
echo             ^<th width^=15%%^>数据库名^</th^>    >>mail_content.html
echo             ^<th width^=12%%^>开始时间^</th^>    >>mail_content.html
echo             ^<th width^=12%%^>完成时间^</th^>^   >>mail_content.html
echo             ^<th width^=5%%^>备份耗时(s)^</th^>  >>mail_content.html
echo             ^<th width^=5%%^>压缩耗时(s)^</th^>  >>mail_content.html
echo             ^<th width^=20^%%^>文件名^</th^>     >>mail_content.html
echo             ^<th width^=5%%^>文件大小^</th^>    >>mail_content.html
echo             ^<th width^=5%%^>完成情况^</th^>    >>mail_content.html
echo             ^<th width^=21%%^>错误消息^</th^>   >>mail_content.html
echo          ^</thead^>    >>mail_content.html
echo          ^<tbody^>     >>mail_content.html
echo           !tbody!      >>mail_content.html
echo          ^</tbody^>    >>mail_content.html
echo       ^</table^>    >>mail_content.html
echo      ^<h4^>总计耗时 :%total_consume%^</h4^> >>mail_content.html 
echo   ^<body^>  >>mail_content.html
echo  ^</html^>  >>mail_content.html
@echo on
rem send email from python script
E:\devops\Python27\python E:\devops\sendmail.py
del mail_content.html
del error.log