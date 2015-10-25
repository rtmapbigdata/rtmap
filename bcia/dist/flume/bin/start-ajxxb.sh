#!/bin/bash

. ./config.sh
flume-ng agent --conf-file ./ajxxb.conf --name agent2 --conf $FLUME_HOME/conf -Dflume.monitoring.type=http -Dflume.monitoring.port=41412 -Dflume.root.logger=INFO,console
