#!/bin/bash

. ./config.sh

rm -rf ${status_file_path}/*
rm -rf ${plugin_dir}/sql-source/lib/*
rm -rf ${plugin_dir}/sql-source/libext/*

mkdir -p ${plugin_dir}/sql-source/lib 
mkdir -p ${plugin_dir}/sql-source/libext

cp -v ${ojdbc_path}/ojdbc7.jar ${plugin_dir}/sql-source/libext
cp -v ${sql_source_jar} ${plugin_dir}/sql-source/lib
cp -v -i ./flume-env.sh $FLUME_HOME/conf

# for debug
tree ${plugin_dir}
cat $FLUME_HOME/conf/flume-env.sh
