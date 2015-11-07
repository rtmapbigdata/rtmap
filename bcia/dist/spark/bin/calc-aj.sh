#!/bin/bash

dates=`date +%Y-%m-%d`
ZkQuorum="r1s2:2181,r1s3:2181,r2s2:2181"
#ZkQuorum="r2s5:2181"
Topics="lkxxb,barcode,ajxxb"
Processing_cycle=600
Lkxxb_times=86400
Hdfs_flag=1
NumThreads=1
Group="1"
Redis_host="r1s2.biginsights.com"
Hdfs_path="hdfs://r2s5/tmp/lkxxb/s/ hdfs://r2s5/tmp/lkxxb/sd/ hdfs://r2s5/tmp/lkxxb/le/ hdfs://r2s5/tmp/lkxxb/ hdfs://r2s5/tmp/barcode/ hdfs://r2s5/tmp/ajxxb/"

ulimit -n 65535
nohup spark-submit  --class com.rtmap.streaming.FlightReport --master spark://r1s2.biginsights.com:7077,r2s4.biginsights.com:7077 --executor-memory 2G --total-executor-cores 4 /home/rtmap/bcia-queue/dist/jars/airport-1.0-SNAPSHOT.jar $Processing_cycle $Topics $ZkQuorum $NumThreads $Group $Hdfs_flag $Lkxxb_times $Redis_host "hdfs://r1s2/bcia-queue/calc/anjian/dura/dura hdfs://r1s2/bcia-queue/calc/anjian/dura_aj/dura_aj hdfs://r1s2/bcia-queue/calc/anjian/dura_level/dura_level hdfs://r1s2/bcia-queue/calc/anjian/dura_barcode/dura_barcode hdfs://r1s2/bcia-queue/calc/anjian/lkxxb/lkxxb hdfs://r1s2/bcia-queue/calc/anjian/barcode/barcode hdfs://r1s2/bcia-queue/calc/anjian/ajxxb/ajxxb" > /home/rtmap/bcia-queue/logs/spark-log/fligt_report_$dates.log 2>&1 &
