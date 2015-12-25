#!/bin/bash

/home/rtmap/unify-ingest/flume-1.6.0/bin/flume-ng agent --name dba1 --conf-file ../conf/db-source.conf  --conf ../conf -Dflume.root.logger=INFO,console