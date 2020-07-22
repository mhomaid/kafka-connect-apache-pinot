#!/usr/bin/env bash

confluent local stop connect
sleep 10

CONN_HOME=/Users/mohamed.homaid/Apache/Pinot/kafka-connect-apache-pinot/target/kafka-connect-target/usr/share/kafka-connect
CONFLUENT_CONN=/Users/mohamed.homaid/Confluent/platform/confluent-5.5.1/share/java/pinot
rm -rf $CONFLUENT_CONN/*
cp -R $CONN_HOME/* $CONFLUENT_CONN
confluent local stop connect
sleep 10
confluent local start connect