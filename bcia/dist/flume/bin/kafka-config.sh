#!/bin/bash

export topic_name=$1
export kafka_home=/usr/iop/current/kafka-broker

# create topic
${kafka_home}/bin/kafka-topics.sh --create --zookeeper r1s4:2181,r1s5:2181,r2s5:2181 --replication-factor 1 --partitions 1 --topic ${topic_name}

# verify topic created successfully
${kafka_home}/bin/kafka-topics.sh --list --zookeeper r1s4:2181,r1s5:2181,r2s5:2181
