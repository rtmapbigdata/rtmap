#!/bin/bash

flume-ng agent -c . -f ./udp.conf -n a1 -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545
