#!/usr/bin/env bash
# Set env 
PINOT_VERSION=0.5.0
PINOT_HOME=/Users/mohamed.homaid/Apache/Pinot/apache-pinot-incubating-0.5.0-SNAPSHOT-bin
cd $PINOT_HOME

#### Start Zookeeper on port 2191 ################
${PINOT_HOME}/bin/pinot-admin.sh StartZookeeper -zkPort 2191
sleep 15
#### Start 2 controllers on ports 9001,9002 ################
${PINOT_HOME}/bin/pinot-admin.sh StartController -zkAddress localhost:2191 -controllerPort 9001
sleep 5
${PINOT_HOME}/bin/pinot-admin.sh StartController -zkAddress localhost:2191 -controllerPort 9000
sleep 10
#### Start 2 brokers on ports 7001,7002 ################
${PINOT_HOME}/bin/pinot-admin.sh StartBroker -zkAddress localhost:2191 -clusterName PinotCluster -brokerPort 7001
sleep 5
${PINOT_HOME}/bin/pinot-admin.sh StartBroker -zkAddress localhost:2191 -clusterName PinotCluster -brokerPort 7002
sleep 10
#### Start 2 servers on ports 8011,8012 ################
${PINOT_HOME}/bin/pinot-admin.sh StartServer -zkAddress localhost:2191 -clusterName PinotCluster -serverPort 8001 -serverAdminPort 8011
${PINOT_HOME}/bin/pinot-admin.sh StartServer -zkAddress localhost:2191 -clusterName PinotCluster -serverPort 8002 -serverAdminPort 8012
sleep 10
######
BASE_DIR=/Users/mohamed.homaid/Apache/Pinot/pinot-tutorial/transcript
cd $BASE_DIR
${PINOT_HOME}/bin/pinot-admin.sh AddTable -tableConfigFile $BASE_DIR/transcript-table-offline.json -schemaFile $BASE_DIR/transcript-schema.json -controllerPort 9001 -exec

#### Load the data #####
${PINOT_HOME}/bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile /Users/mohamed.homaid/Apache/Pinot/pinot-tutorial/transcript/batch-job-spec.yml

${PINOT_HOME}/bin/pinot-admin.sh AddTable \
  -tableConfigFile /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/transcript-table-offline.json \
  -schemaFile /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/transcript-schema.json -controllerPort 9001 -exec

${PINOT_HOME}/bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/batch-job-spec.yml

bin/pinot-admin.sh  StartKafka -zkAddress=localhost:2191/kafka -port 9876
bin/kafka-topics.sh --create --bootstrap-server localhost:9876 --replication-factor 1 --partitions 1 --topic transcript-topic

bin/pinot-admin.sh AddTable \
    -schemaFile /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/transcript-schema.json \
    -tableConfigFile /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/transcript-table-realtime.json \
    -controllerPort 9001 -exec

kafka-console-producer \
    --broker-list localhost:9876 \
    --topic transcript-topic < /Users/mohamed.homaid/Apache/Pinot/pinot-quick-start/rawData/transcript.json