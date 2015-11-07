#!/bin/bash

. ./config.sh

is_launched=`ps -ef | grep flume-ajxxb.log | grep -v 'grep'`
if [ "$is_launched" != "" ]; then
  echo already launched, skip!
else
  nohup flume-ng agent --conf-file ../conf/ajxxb.conf --name agent2 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-ajxxb.log > /dev/null 2>&1 &
fi
