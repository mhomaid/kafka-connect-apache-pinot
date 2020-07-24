package com.yama.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  private Map<String, String> configProps;

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    if (config == null) {
      throw new ConnectException("Connector config has not been initialized.");
    }
    final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      taskConfigs.add(configProps);
    }
    log.debug("{} Partitions grouped as: {}", this, taskConfigs);
    return taskConfigs;
  }

  @Override
  public void start(Map<String, String> properties) {
    log.info("{} Starting connector...", this);
    try {
      configProps = properties;
      config = new PinotSinkConnectorConfig(configProps);
    } catch (ConfigException ce) {
      throw new ConnectException("Couldn't start PinotSinkConnector due to configuration error.", ce);
    } catch (Exception ce) {
      throw new ConnectException("An error has occurred when starting PinotSinkConnector." + ce);
    }
  }


  @Override
  public void stop() {
    log.info("{} Stopping PinotSinkConnector ... ", this);
  }

  @Override
  public ConfigDef config() {
    return PinotSinkConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return PinotSinkTask.class;
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
