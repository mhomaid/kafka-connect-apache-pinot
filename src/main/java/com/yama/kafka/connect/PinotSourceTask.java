package com.yama.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PinotSourceTask extends SourceTask {

  static final Logger log = LoggerFactory.getLogger(PinotSourceTask.class);
  public PinotSourceConnectorConfig config;
  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> map) {
    config = new PinotSourceConnectorConfig(map);

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    // fetch data
    final ArrayList<SourceRecord> records = new ArrayList<>();

    records.add(generateSourceRecord());
    return records;
  }

  private SourceRecord generateSourceRecord() {
    return new SourceRecord(
            null,
            null,
            config.kafkaTopic,
            null, // partition will be inferred by the framework
            null,
            "Hello",
            null,
            "World",
            null);
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}