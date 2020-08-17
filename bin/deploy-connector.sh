#!/usr/bin/env bash
mvn clean package
confluent local services connect stop
#sleep 10

PINOT_CONN_HOME=/Users/mohamed.homaid/Apache/Pinot/kafka-connect-apache-pinot/target/kafka-connect-target/usr/share/kafka-connect
CONNECT_HOME_DIR=/Users/mohamed.homaid/Confluent/platform/confluent-5.5.1/share/java/pinot
#rm -rf $CONFLUENT_CONN/*
cp -R $PINOT_CONN_HOME/* $CONNECT_HOME_DIR
confluent local services connect start
