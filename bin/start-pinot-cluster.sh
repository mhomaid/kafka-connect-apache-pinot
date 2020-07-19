#!/usr/bin/env bash
# Set env 
PINOT_VERSION=0.4.0
PINOT_HOME=~/Apache/Pinot/apache-pinot-incubating-${PINOT_VERSION}-bin

#### Start Zookeeper on port 2191 ################
${PINOT_HOME}/bin/pinot-admin.sh StartZookeeper -zkPort 2191
sleep 15
#### Start 2 controllers on ports 9001,9002 ################
${PINOT_HOME}/bin/pinot-admin.sh StartController -zkAddress localhost:2191 -controllerPort 9001
sleep 5
${PINOT_HOME}/bin/pinot-admin.sh StartController -zkAddress localhost:2191 -controllerPort 9002
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
BASE_DIR=~/Apache/Pinot/pinot-tutorial/transcript
${PINOT_HOME}/bin/pinot-admin.sh AddTable -tableConfigFile $BASE_DIR/transcript-table-offline.json -schemaFile $BASE_DIR/transcript-schema.json -controllerPort 9001 -exec

#### Load the data #####
${PINOT_HOME}/bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile ~/Apache/Pinot/pinot-tutorial/transcript/batch-job-spec.yml

${PINOT_HOME}/bin/pinot-admin.sh AddTable \
  -tableConfigFile ~/Apache/Pinot/pinot-quick-start/transcript-table-offline.json \
  -schemaFile ~/Apache/Pinot/pinot-quick-start/transcript-schema.json -controllerPort 9001 -exec

${PINOT_HOME}/bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile ~/Apache/Pinot/pinot-quick-start/batch-job-spec.yml