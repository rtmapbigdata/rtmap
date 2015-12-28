#!/bin/bash

nohup flume-ng agent --name a1 --conf-file ../conf/kafka-source.conf  --conf ../conf -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=../log -Dflume.log.file=kafka-source.log > /dev/null 2>&1 &
