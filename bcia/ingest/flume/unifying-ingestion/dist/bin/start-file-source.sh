#!/bin/bash

nohup /home/rtmap/unify-ingest/flume-1.6.0/bin/flume-ng agent --name a1 --conf-file ../conf/file-source.conf  --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=file-source.log > /dev/null 2>&1 &
