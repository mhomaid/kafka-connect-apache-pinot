package com.yama.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class PinotSinkConnectorConfig extends AbstractConfig {

  public static final String TOPIC_CONFIG = "kafka.topic";
  private static final String TOPIC_DOC = "Kafka Topic consumer message from";

  public static final String PINOT_ZOOKEEPER_CONFIG = "pinot.zookeeper.nodes";
  private static final String PINOT_ZOOKEEPER_DOC = "List of Pinot Zookeepers";

  public static final String PINOT_BROKERS_CONFIG = "pinot.brokers.nodes";
  private static final String PINOT_BROKERS_CONFIG_DOC = "List of Pinot Brokers";
  public static final String CONNECTOR_GROUP = "Pinot Cluster Configs";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";


  public final String kafkaTopic;
  public final String batchSize;
  public final String pinotZookeepers;
  public final String pinotBrokers;

  public PinotSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.kafkaTopic = this.getString(TOPIC_CONFIG);
    this.batchSize = this.getString(BATCH_SIZE_CONFIG);
    this.pinotZookeepers = this.getString(PINOT_ZOOKEEPER_CONFIG);
    this.pinotBrokers = this.getString(PINOT_BROKERS_CONFIG);
  }

  public static ConfigDef config() {
    ConfigDef config = new ConfigDef();
    addPinotClusterOptions(config);
    return config;
  }

  private static void addPinotClusterOptions(ConfigDef config) {
    int orderInGroup = 0;
    config.define(
            ConfigKeyBuilder.of(
                    TOPIC_CONFIG,
                    Type.STRING)
                    .documentation(TOPIC_DOC)
                    .importance(Importance.HIGH)
                    .orderInGroup(++orderInGroup)
                    .group(CONNECTOR_GROUP)
                    .build())
            .define(
                    ConfigKeyBuilder.of(PINOT_ZOOKEEPER_CONFIG, Type.STRING)
                            .documentation(PINOT_ZOOKEEPER_DOC)
                            .importance(Importance.LOW)
                            .orderInGroup(++orderInGroup)
                            .group(CONNECTOR_GROUP)
                            .build())
            .define(
                    ConfigKeyBuilder.of(PINOT_BROKERS_CONFIG, Type.STRING)
                            .documentation(PINOT_BROKERS_CONFIG_DOC)
                            .importance(Importance.LOW)
                            .orderInGroup(++orderInGroup)
                            .group(CONNECTOR_GROUP)
                            .build())
            .define(
                    ConfigKeyBuilder.of(BATCH_SIZE_CONFIG, Type.STRING)
                            .documentation(BATCH_SIZE_DOC)
                            .importance(Importance.LOW)
                            .orderInGroup(++orderInGroup)
                            .group(CONNECTOR_GROUP)
                            .build()
            );
  }
}
