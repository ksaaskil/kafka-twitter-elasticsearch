#!/usr/bin/env bash

set -uex

# Modify this for your needs
KAFKA_DIR=${HOME}/kafka_2.12-2.4.0

bash ${KAFKA_DIR}/bin/zookeeper-server-start.sh ${KAFKA_DIR}/config/zookeeper.properties > zookeeper.log &

bash ${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/server.properties > kafka.log &
