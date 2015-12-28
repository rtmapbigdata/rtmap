#!/bin/bash

flume-ng agent --name a4 --conf-file ../conf/udp.conf --conf ../conf -Dflume.root.logger=INFO,console