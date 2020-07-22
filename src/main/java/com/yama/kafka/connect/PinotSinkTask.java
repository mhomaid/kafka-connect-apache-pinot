package com.yama.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class PinotSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(PinotSinkTask.class);

  PinotSinkConnectorConfig config;
  String batchSize;
  @Override
  public void start(Map<String, String> props) {
    log.info("Starting Pinot Sink task");
    this.config = new PinotSinkConnectorConfig(props);
    batchSize = config.batchSize;
    //TODO: Create api connections here.
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.info("Put method started");
    if (records.isEmpty()) {
      return;
    }
    for (SinkRecord record : records) {
      log.info("record {} .. ", record);
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
    }
    log.info("Read {} records from Kafka", records.size());

    try{
      //TODO : Create a writer and segment and push the records to it
    }catch (Exception e){

    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void stop() {
    log.info("Stopping task");
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
