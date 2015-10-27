#!/bin/bash

. ./config.sh
flume-ng agent --conf-file ../conf/lkxxb.conf --name agent3 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-lkxxb.log
