#!/bin/bash

nohup flume-ng agent --name exchange --conf-file ../conf/exchange-inner.conf  --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=exchange-inner.log > /dev/null 2>&1 &
