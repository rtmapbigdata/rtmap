#!/bin/bash

export FLUME_HOME=/usr/iop/current/flume-server

is_launched=`ps -ef | grep flume-lbs.log | grep -v 'grep'`
if [ "$is_launched" != "" ]; then
  echo already launched, skip!
else
  nohup flume-ng agent --conf-file ../conf/lbs.conf --name agent4 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-lbs.log > /dev/null 2>&1 &  
fi
