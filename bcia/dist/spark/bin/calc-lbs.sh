#!/bin/bash

dates=`date +%Y-%m-%d`
ZkQuorum="r1s2:2181,r1s3:2181,r2s2:2181"
#ZkQuorum="r2s5:2181"
Topics="lbs"
Processing_cycle=600
Lbs_times=4200
Hdfs_flag=1
NumThreads=1
Group="1"
Redis_host="r1s2.biginsights.com"
Hdfs_path="hdfs://r2s5/tmp/wifi/re/ hdfs://r2s5/tmp/wifi/lbs/"

ulimit -n 65535
nohup spark-submit  --class com.rtmap.streaming.WifiReport --master spark://r1s2.biginsights.com:7077,r2s4.biginsights.com:7077 --executor-memory 8G --total-executor-cores 12 /home/rtmap/bcia-queue/dist/jars/airport-1.0-SNAPSHOT.jar $Processing_cycle $Topics $ZkQuorum $NumThreads $Group $Lbs_times $Hdfs_flag $Redis_host hdfs://r1s2/bcia-queue/calc/wifi/dura/dura hdfs://r1s2/bcia-queue/calc/wifi/dura_level/dura_level hdfs://r1s2/bcia-queue/calc/wifi/lbs/lbs > /home/rtmap/bcia-queue/logs/spark-log/wifi_report_$dates.log 2>&1 &
