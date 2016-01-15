#!/bin/bash

nohup /home/rtmap/unify-ingest/flume-1.6.0/bin/flume-ng agent --name dbamarketv3 --conf-file ../conf/db-source-market-v3.conf  --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=db-source-market-v3.log > /dev/null 2>&1 &