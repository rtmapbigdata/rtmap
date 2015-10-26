#!/bin/bash

export KAFKA_HOME=/usr/iop/current/kafka-broker
export FLUME_HOME=/usr/iop/current/flume-server
export KAFKA_CLIENT=kafka-clients-0.8.2.1-IBM-5.jar

mvn clean package -DskipTests
cd dist/target

unzip ./flume-kafka-sink-dist-0.5.0-bin.zip
cd flume-kafka-sink-dist-0.5.0/lib

rm -rf ${FLUME_HOME}/plugins.d/kafka-sink/lib
rm -rf ${FLUME_HOME}/plugins.d/kafka-sink/libext

mkdir -p ${FLUME_HOME}/plugins.d/kafka-sink/lib
mkdir -p ${FLUME_HOME}/plugins.d/kafka-sink/libext

cp ./flume-kafka-sink-impl-0.5.0.jar ${FLUME_HOME}/plugins.d/kafka-sink/lib
cp ./kafka_2.10-0.8.1.1.jar ${FLUME_HOME}/plugins.d/kafka-sink/libext
cp ./metrics-core-2.2.0.jar ${FLUME_HOME}/plugins.d/kafka-sink/libext
cp ./scala-library-2.10.1.jar ${FLUME_HOME}/plugins.d/kafka-sink/libext

cd ../../../../example/target
cp ./flume-kafka-sink-example-0.5.0.jar ${FLUME_HOME}/plugins.d/kafka-sink/libext

# fix error: class not found - org.apache.kafka.common.utils.Utils
cp ${KAFKA_HOME}/libs/${KAFKA_CLIENT} ${FLUME_HOME}/plugins.d/kafka-sink/libext

tree ${FLUME_HOME}/plugins.d/kafka-sink
