#!/bin/bash

. ./config.sh
flume-ng agent --conf-file ../conf/barcode.conf --name agent1 --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../../logs/flume-log -Dflume.log.file=flume-barcode.log
