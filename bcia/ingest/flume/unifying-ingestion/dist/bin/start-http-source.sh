#!/bin/bash

/home/rtmap/unify-ingest/flume-1.6.0/bin/flume-ng agent --name a4 --conf-file ../conf/http-source.conf --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=http-source.log > /dev/null 2>&1 &
