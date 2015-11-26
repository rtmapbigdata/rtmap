#!/bin/bash

export FLUME_HOME=/usr/hdp/current/flume-server
export is_launched=`ps -ef | grep flume-electrocar.log | grep -v 'grep'`

if [ "$is_launched" != "" ]; then
  echo already launched, skip!
else
  nohup flume-ng agent --conf-file ../conf/electrocar.conf --name a2 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-electrocar.log > /dev/null 2>&1 &
fi
