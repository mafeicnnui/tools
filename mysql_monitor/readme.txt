一、安装步骤

（1）新建目录
 mkdir /home/dba

（2）安装python3.6

tar zxvf python3.6.0.tar.gz -C /usr/local
export LD_LIBRARY_PATH=/usr/local/python3.6.0/lib

（3）安装无头浏览器，用于后台生成图表
cd /home/dba
unzip plugin.zip
chmod +x /home/dba/plugin/bin/phantomjs
yum -y install fontconfig
yum install bitmap-fonts bitmap-fonts-cjk
yum -y install zip unzip

（4）验证是否安装成功
/usr/local/python3.6.0/bin/python3 -V  #
Python 3.6.0

（5）安装python插件
wget https://files.pythonhosted.org/packages/44/39/6bcb83cae0095a31b6be4511707fdf2009d3e29903a55a0494d3a9a2fac0/PyMySQL-0.8.1.tar.gz
tar zxvf PyMySQL-0.8.1.tar.gz 
cd PyMySQL-0.8.1/
python setup.py install
pip3 install xlwt


（6） 执行脚本，创建监控需要的相关表
devops.sql

二、启动服务

（1）方法一，用于调试脚本
export LD_LIBRARY_PATH=/usr/local/python3.6.0/lib
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini -mode alone

（2）方法二，运行start_monitor.sh后台方式进行
/home/dba/start_monitor.sh


三、设置定时任务
*/1 * * * * /home/dba/check_monitor.sh &   


四 、使用说明

（1）解析二进制日志
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini -mode binlog

（2）数据库监控
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini -mode alone
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini -mode cluster

（3）重建索引
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini  -mode gather_index
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini  -mode rebuild_index

（4）数据迁移
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini  -mode transfer
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini  -mode transfer -debug

（5）数据导出
/usr/local/python3.6.0/bin/python3 /home/dba/mysql_monitor.py -conf /home/dba/mysql_monitor.ini -mode exp


五、数据库监控

1.sh: /home/dba/prod/plugin/bin/phantomjs: Permission denied
v_files= ['/home/dba/prod/plugin/image/echarts0501.png', '/home/dba/prod/plugin/image/echarts0502.png']

chmod +x /home/dba/prod/plugin/bin/phantomjs

2./home/dba/prod/plugin/bin/phantomjs: error while loading shared libraries: libfontconfig.so.1
yum -y install fontconfig

3. 用phantomjs截图时中文乱码的解决方案
yum install bitmap-fonts bitmap-fonts-cjk

4.sh: zip: command not found
yum -y install zip unzip

