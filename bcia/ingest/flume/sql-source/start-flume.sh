#!/bin/bash

export FLUME_HOME=/usr/hdp/current/flume-server
# export JAVA_OPTS="-Xms100m -Xmx4096m"

rm -rf /var/lib/flume/*
rm -rf $FLUME_HOME/plugins.d/sql-source/lib/*
cp -v target/flume-ng-sql-source-1.3-SNAPSHOT.jar $FLUME_HOME/plugins.d/sql-source/lib

flume-ng agent --conf-file ./sql-source.conf --name agent2 --conf $FLUME_HOME/conf -Dflume.monitoring.type=http -Dflume.monitoring.port=41412 -Dflume.root.logger=INFO,console
