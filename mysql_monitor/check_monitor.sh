#!/bin/sh
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export MONITOR_HOME=/home/hopson/apps/usr/webserver/dba/monitor/bi
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
i_counter=`ps -ef | grep mysql_monitor.py | grep -v grep | grep -v vi | grep bi | wc -l`
v_cmd="${MONITOR_HOME}/start_monitor.sh"
echo 'i_counter=' ${i_counter}

if [ "${i_counter}" -eq "0" ]; then 
   echo 'starting MySQL monitor...'
   echo "${v_cmd}" 
  `${v_cmd}`
   echo 'starting MySQL monitor....success!'
fi
