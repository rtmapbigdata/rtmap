#!/bin/bash

. ./config.sh
flume-ng agent --conf-file ../conf/ajxxb.conf --name agent2 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-ajxxb.log
