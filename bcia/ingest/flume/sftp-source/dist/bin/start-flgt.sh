#!/bin/bash

#export FLUME_HOME=/usr/hdp/current/flume-server
export is_launched=`ps -ef | grep flume-flgt.log | grep -v 'grep'`

if [ "$is_launched" != "" ]; then
  echo already launched, skip!
else
  flume-ng agent --conf-file ../conf/flgt.conf --name a1 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-flgt.log
fi
