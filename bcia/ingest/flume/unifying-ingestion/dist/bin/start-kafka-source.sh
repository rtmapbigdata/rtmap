#!/bin/bash

flume-ng agent --name a2 --conf-file ../conf/kafka-source.conf  --conf ../conf -Dflume.root.logger=INFO,console
