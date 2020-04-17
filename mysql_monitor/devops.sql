SET GLOBAL event_scheduler = 1 ; 

create database devops character set utf8 ;
use devops
create table heart(x datetime); 
insert into  heart(x) values(now());


CREATE TABLE `t_slow_sql` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `esalpse_time` int(11) DEFAULT NULL,
  `statement` text,
  `created_date` datetime DEFAULT NULL,
  `db` varchar(100) DEFAULT NULL,
  `host` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_t_slow_sql_created_date` (`created_date`)
) ENGINE=InnoDB AUTO_INCREMENT=227619 DEFAULT CHARSET=utf8;


create TABLE t_tab_rate(
   id int not NULL AUTO_INCREMENT primary KEY ,
   schema_name varchar(100),
   table_name  varchar(100) not NULL,
   table_rows int not NULL,
   table_size int not NULL,
   tjrq datetime not null  
);

CREATE TABLE `t_db_monitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `connection_total` int(11) DEFAULT NULL,
  `connection_active` int(11) DEFAULT NULL,
  `tjrq` datetime DEFAULT NULL,
  `host` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=198 DEFAULT CHARSET=utf8;

CREATE VIEW v_active_thread AS 
SELECT c.id AS thread_id,	    
	  GROUP_CONCAT(CONCAT(LTRIM(REPLACE(a.sql_text,'EXPLAIN','')),'\n ') SEPARATOR '') AS sql_text		
FROM performance_schema.events_statements_current a,performance_schema.threads b,information_schema.processlist c 
WHERE a.thread_id=b.thread_id
  AND b.processlist_id=c.id
GROUP BY c.id;

CREATE TABLE t_db_indexes(
	id INT PRIMARY KEY AUTO_INCREMENT,
	schema_name VARCHAR(100),
	table_name VARCHAR(100),
	non_unique VARCHAR(1),
	key_name VARCHAR(100),
	seq_in_index INT,
	column_name VARCHAR(100),
	is_null VARCHAR(10),
	index_type VARCHAR(20),
    created datetime	
);
	
CREATE TABLE `t_db_backup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `mail_title` varchar(100) DEFAULT NULL,
  `mail_content` text,
  `flag` varchar(20) DEFAULT NULL,
  `message` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


#MySQL 5.6：v_index_gather
CREATE VIEW v_index_gather AS 
  SELECT schema_name,table_name,key_name,index_type,GROUP_CONCAT(column_name) AS index_columns      
   FROM devops.t_db_indexes
  GROUP BY schema_name,table_name,key_name,index_type
  ORDER BY schema_name,table_name,seq_in_index;

#MySQL 5.7：v_index_gather
CREATE VIEW v_index_gather AS 
	SELECT schema_name,table_name,key_name,index_type,GROUP_CONCAT(column_name) AS index_columns      
	 FROM (SELECT * FROM devops.t_db_indexes ORDER BY schema_name,table_name,seq_in_index) X
	GROUP BY schema_name,table_name,key_name,index_type;
   
--查询重复索引
SELECT schema_name,table_name,index_columns,
       GROUP_CONCAT(key_name) AS index_name,
       COUNT(0) 
 FROM devops.v_index_gather 
 WHERE key_name!='PRIMARY'
GROUP BY schema_name,table_name,index_columns
HAVING COUNT(0)>1
ORDER BY schema_name,table_name,index_columns;

--创建定时任务，需要打开开关：event_scheduler = 1
DELIMITER $$                                                                   
create event event_sync_heart on SCHEDULE EVERY 3 second DO                    
BEGIN                                                                          
  update heart set x=now();                                                    
END$$                                                                          
DELIMITER ;    

CREATE TABLE `t_metadata_lock` (
  `id` BIGINT(21) UNSIGNED DEFAULT NULL,
  `USER` VARCHAR(32) DEFAULT NULL,
  `HOST` VARCHAR(64) DEFAULT NULL,
  `db` VARCHAR(64) DEFAULT NULL,
  `command` VARCHAR(16) DEFAULT NULL,
  `TIME` INT(7) DEFAULT NULL,
  `state` VARCHAR(64) DEFAULT NULL,
  `info` LONGTEXT,
  `lock_type` VARCHAR(50) DEFAULT NULL,
  `STATUS` VARCHAR(64) DEFAULT NULL,
  `start_time` DATETIME DEFAULT NULL,
  `gather_time` DATETIME DEFAULT NULL,
  `send_mail` VARCHAR(1) DEFAULT NULL
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `t_metadata_lock_table` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `db` VARCHAR(100) DEFAULT NULL,
  `tname` VARCHAR(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=INNODB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8;

CREATE TABLE `t_db_binlog` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `log_name` VARCHAR(100) DEFAULT NULL,
  `db` VARCHAR(100) DEFAULT NULL,
  `begin_pos` INT(11) DEFAULT NULL,
  `end_pos` INT(11) DEFAULT NULL,
  `server_id` INT(11) DEFAULT NULL,
  `event_type` VARCHAR(100) DEFAULT NULL,
  `statement` LONGTEXT,
  PRIMARY KEY (`id`)
) ENGINE=INNODB AUTO_INCREMENT=3954591 DEFAULT CHARSET=utf8;
  

--创建监控用户，mysqldump权限
grant select,process on *.* to 'puppet'@'%'  identified by 'Puppet@123';
grant all privileges on devops.* to 'puppet'@'%' ;
grant select on *.* to puppet@'%';
grant show view on *.* to puppet@'%';
grant lock tables on *.* to puppet@'%';
grant trigger on *.* to puppet@'%';
grant REPLICATION SLAVE on *.*  to 'puppet'@'%'; #pt-online-schema

--------------------------------------------20181128-----------------------------------------
SELECT  
    CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,';')   drop_index,
    CONCAT('alter table ',schema_name,'.',table_name,' add index ' ,key_name,'(',index_columns,') using ',index_type,';') AS cre_index,
    CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,',add index ',key_name,'(',index_columns,') using ',index_type,';') AS cre_index2      
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY'
 AND table_name='mdm_store'
ORDER BY schema_name,table_name,key_name;   

SELECT  
   CONCAT('pt-online-schema-change -u root -p winchannel --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "drop index ',key_name,'",D=',schema_name,',t=',table_name)  AS drop_index,
   CONCAT('pt-online-schema-change -u root -p winchannel --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "add index ',key_name,'(',index_columns,')",D=',schema_name,',t=',table_name)  AS cre_index   
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao' AND table_name='mod_di_v2_log_step' AND key_name<>'PRIMARY'
ORDER BY schema_name,table_name;


/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode gather_index
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode rebuild_index

pt-online-schema-change -u root -p winchannel --execute --print --progress TIME,5 --charset=utf8 --alter-FOREIGN-KEYS-method=rebuild_constraints --alter "drop index FK8DABE3AD8C41E7DF",D=sfa_yihaijiali_fenxiao,t=mod_di_v2_log_step
pt-online-schema-change -u root -p winchannel --execute --print --progress TIME,5 --charset=utf8 --alter-FOREIGN-KEYS-method=rebuild_constraints --alter "add index FK8DABE3AD8C41E7DF(transform_log_id)",D=sfa_yihaijiali_fenxiao,t=mod_di_v2_log_step

---------------------------------------------normal-------------------------------------------------------
SELECT  schema_name,table_name,key_name,
        CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,';')   AS statement
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY'
ORDER BY schema_name,table_name,key_name;

SELECT  schema_name,table_name,key_name, 
	CASE WHEN index_type='FULLTEXT' THEN
	     CONCAT('alter table ',schema_name,'.',table_name,' add ',index_type,' index ',key_name,'(',index_columns,');') 
	ELSE
	     CONCAT('alter table ',schema_name,'.',table_name,' add index ',key_name,'(',index_columns,') using ',index_type,';') END AS  statement   
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY'
ORDER BY schema_name,table_name,key_name;

------------------------------------------------pt-------------------------------------------------------
SELECT  schema_name,table_name,key_name,
      CONCAT('pt-online-schema-change -u puppet -p Puppet@123 --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "drop index ',key_name,'" D=',schema_name,',t=',table_name)  AS drop_index
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY'
ORDER BY schema_name,table_name,key_name;

SELECT  schema_name,table_name,key_name,
	CASE WHEN index_type='FULLTEXT' THEN
           CONCAT('pt-online-schema-change -u puppet -p Puppet@123 --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "add fulltext index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name)   
	ELSE
           CONCAT('pt-online-schema-change -u puppet -p Puppet@123 --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "add index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name) END  AS cre_index   
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY'
ORDER BY schema_name,table_name,key_name;

---------------------------------------------2018.12.05----------------------------------------------------

SELECT
	CASE WHEN index_type='FULLTEXT' THEN
	  CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,', add ',index_type,' index ',key_name,'(',index_columns,')')	 
	ELSE
	  CONCAT('alter table ',schema_name,'.',table_name,' drop index ',key_name,', add index ',key_name,'(',index_columns,') using ',index_type) END AS  statement
FROM devops.v_index_gather
WHERE schema_name='sfa_yihaijiali_fenxiao' AND index_type='FULLTEXT'  AND key_name!='PRIMARY'
ORDER BY schema_name,table_name,key_name

SELECT  schema_name,table_name,key_name,
	CASE WHEN index_type='FULLTEXT' THEN
           CONCAT('pt-online-schema-change -u puppet -p Puppet@123 --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "drop index ',key_name,', add fulltext index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name)   
	ELSE
           CONCAT('pt-online-schema-change -u puppet -p Puppet@123 --execute --print --progress time,5 --charset=utf8 --alter-foreign-keys-method=rebuild_constraints --alter "drop index ',key_name,', add index ',key_name,'(',index_columns,')" D=',schema_name,',t=',table_name) END  AS cre_index   
FROM devops.v_index_gather 
WHERE schema_name='sfa_yihaijiali_fenxiao'  AND key_name!='PRIMARY' AND index_type='FULLTEXT'
ORDER BY schema_name,table_name,key_name;


/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode rebuild_indexes




#表传输
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode transfer
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode transfer -debug
MySQL transfer parameter...
-------------------------------------------------------------------------------------
   name                value
-------------------------------------------------------------------------------------
   db_sour_ip          = 192.168.1.48
   db_sour_port        = 3306
   db_sour_service     = devops
   db_sour_user        = puppet
   db_sour_pass        = Puppet@123
   db_dest_ip          = 192.168.1.161
   db_dest_port        = 3306
   db_dest_service     = devops
   db_dest_user        = puppet
   db_dest_pass        = Puppet@123
   db_sour_string      = 192.168.1.48:3306/devops
   db_dest_string      = 192.168.1.161:3306/devops
   transfer_sour_db    = sfa_yihaijiali_fenxiao_20181120
   transfer_dest_db    = sfa_yihaijiali_fenxiao_20181120
   db_sour             = <pymysql.connections.Connection object at 0x7fb67f0ec668>
   db_dest             = <pymysql.connections.Connection object at 0x7fb67f104c88>
   transfer_table      = mod_di_v2_log_rowdata
   transfer_tab_part_range= p20190116~p20190117
-------------------------------------------------------------------------------------

#binlog解析
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode binlog -debug

#重建索引
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode gather_index
/usr/local/python3.6.0/bin/python3 /home/dba/3306/mysql_monitor.py -conf /home/dba/3306/mysql_monitor.ini  -mode rebuild_index


#数据库归档设计 

--------------------------------------------------------------monitor---------------------------------------------------------------

一、将打好的python3二进制包上传至服务器上
python3.6.0.tar.gz

二、解村二进制包
tar zxvf python3.6.0.tar.gz

三、设置python3
export LD_LIBRARY_PATH=/usr/local/python3.6.0/lib

三、启动服务

export LD_LIBRARY_PATH=/usr/local/python3.6.0/lib
nohup /usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini  -mode alone &

四、设置定时任务
*/1 * * * * /home/dba/3307/check_monitor.sh &