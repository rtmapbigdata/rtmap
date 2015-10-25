#!/bin/bash

. ./config.sh
flume-ng agent --conf-file ./barcode.conf --name agent1 --conf $FLUME_HOME/conf -Dflume.monitoring.type=http -Dflume.monitoring.port=41412 -Dflume.root.logger=INFO,console
