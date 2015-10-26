#/bin/bash

ZkQuorum="r1s4:2181,r1s5:2181,r2s5:2181"
#ZkQuorum="r2s5:2181"
Topics="lkxxb_2,barcode_2,ajxxb_2"
Processing_cycle=600
Lkxxb_times=86400
Hdfs_flag=1
NumThreads=1
Group="1"
Hdfs_path="hdfs://r2s5/tmp/lkxxb/s/ hdfs://r2s5/tmp/lkxxb/sd/ hdfs://r2s5/tmp/lkxxb/le/ hdfs://r2s5/tmp/lkxxb/ hdfs://r2s5/tmp/barcode/ hdfs://r2s5/tmp/ajxxb/"

#spark-submit  --class com.rtmap.streaming.WifiReport --master spark://r2s5:7077 --executor-memory 512M --total-executor-cores 1 airport-1.0-SNAPSHOT.jar $ZkQuorum $Topics $NumThreads $Group $Processing_cycle $Data_cycle $Hdfs_flag $Hdfs_path

ulimit -n 65535
nohup spark-submit  --class com.rtmap.streaming.FlightReport --master spark://r2s5:7077 --executor-memory 2G --total-executor-cores 2 airport-1.0-SNAPSHOT.jar $Processing_cycle $Topics $ZkQuorum $NumThreads $Group $Hdfs_flag $Lkxxb_times "hdfs://r2s5/tmp/lkxxb/s/ hdfs://r2s5/tmp/lkxxb/sd/ hdfs://r2s5/tmp/lkxxb/le/ hdfs://r2s5/tmp/lkxxb/lkxxb/ hdfs://r2s5/tmp/lkxxb/barcode/ hdfs://r2s5/tmp/lkxxb/ajxxb/" > fligt_report.log 2>&1 &
