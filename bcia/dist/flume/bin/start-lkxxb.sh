#!/bin/bash

. ./config.sh

is_launched=`ps -ef | grep flume-lkxxb.log | grep -v 'grep'`
if [ "$is_launched" != "" ]; then
  echo already launched, skip!
else
  nohup flume-ng agent --conf-file ../conf/lkxxb.conf --name agent3 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-lkxxb.log > /dev/null 2>&1 &
fi
