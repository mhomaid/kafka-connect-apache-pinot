package com.yama.kafka.connect;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

/**
 *
 */

@Description("kafka-connect-apache-pinot is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)\n" +
        "for loading data to and from any [Apache Pinot](https://pinot.apache.org/).")
@DocumentationImportant("This is a important information that will show up in the documentation.")
@DocumentationTip("This is a tip that will show up in the documentation.")
@Title("Kafka Connect Apache Pinot Sink Connector")
@DocumentationNote("This is a note that will show up in the documentation")
public class PinotSinkConnector extends SinkConnector {

  private static Logger log = LoggerFactory.getLogger(PinotSinkConnector.class);
  private PinotSinkConnectorConfig config;

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    //TODO: Define the individual task configurations that will be executed.

    /**
     * This is used to schedule the number of tasks that will be running. This should not exceed maxTasks.
     * Here is a spot where you can dish out work. For example if you are reading from multiple tables
     * in a database, you can assign a table per task.
     */

    throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void start(Map<String, String> settings) {
    config = new PinotSinkConnectorConfig(settings);

    //TODO: Add things you need to do to setup your connector.

    /**
     * This will be executed once per connector. This can be used to handle connector level setup. For
     * example if you are persisting state, you can use this to method to create your state table. You
     * could also use this to verify permissions
     */

  }


  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return PinotSinkConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return PinotSinkTask.class;
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
