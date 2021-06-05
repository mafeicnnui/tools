#!/usr/bin/env bash
export WORKDIR='/home/hopson/apps/usr/webserver/dba/kpi_bi'
export PYTHONUNBUFFERED="1"
export PYTHONPATH=${WORKDIR}
export PYTHON3_HOME=/usr/local/python3.6
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
echo "Starting projects kpi stats task..."
cd $WORKDIR
${PYTHON3_HOME}/bin/python3 -u ${WORKDIR}/gen_reporter.py
