#!/bin/bash

/home/rtmap/unifying-ingestion/flume-1.6.0/bin/flume-ng agent --name a1 --conf-file ../conf/file-source.conf  --conf ../conf -Dflume.root.logger=DEBUG,console