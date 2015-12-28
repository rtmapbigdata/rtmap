#!/bin/bash

nohup /home/rtmap/unify-ingest/flume-1.6.0/bin/flume-ng agent --name a3 --conf-file ../conf/udp-source.conf --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=udp-source.log > /dev/null 2>&1 &
