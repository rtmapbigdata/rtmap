#/bin/bash

ZkQuorum="r1s4:2181,r1s5:2181,r2s5:2181"
#ZkQuorum="r2s5:2181"
Topics="lbs"
Processing_cycle=600
Lbs_times=4200
Hdfs_flag=1
NumThreads=1
Group="1"
Hdfs_path="hdfs://r2s5/tmp/wifi/re/ hdfs://r2s5/tmp/wifi/lbs/"

ulimit -n 65535
nohup spark-submit  --class com.rtmap.streaming.WifiReport --master spark://r2s5:7077 --executor-memory 2G --total-executor-cores 8 airport-1.0-SNAPSHOT.jar $Processing_cycle $Topics $ZkQuorum $NumThreads $Group $Lbs_times $Hdfs_flag hdfs://r2s5/tmp/wifi/re/ hdfs://r2s5/tmp/wifi/s/ hdfs://r2s5/tmp/wifi/lbs/ > wifi_report.log 2>&1 &

