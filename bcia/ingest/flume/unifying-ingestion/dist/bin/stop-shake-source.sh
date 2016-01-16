#!/bin/bash

export LOGNAME=shake-source.log
export PID0=`ps -ef | grep -v grep | grep $LOGNAME | awk '{print $2}'`

echo ${PID0}

if [ ! "${PID0}" = "" ]; then
  ps -ef | grep $LOGNAME | grep -v grep | awk '{print $2}' | xargs kill -9
  echo killed process $PID0
else
  echo no process found for $LOGNAME
fi
