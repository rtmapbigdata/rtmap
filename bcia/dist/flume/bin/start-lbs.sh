#!/bin/bash

export FLUME_HOME=/usr/iop/current/flume-server
flume-ng agent --conf-file ../conf/lbs.conf --name agent4 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-lbs.log
