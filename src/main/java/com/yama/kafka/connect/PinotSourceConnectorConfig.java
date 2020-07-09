package com.yama.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import java.util.Map;



public class PinotSourceConnectorConfig extends AbstractConfig {

  public static final String TOPIC_CONFIG = "kafka.topic";
  private static final String TOPIC_DOC = "Kafka Topic to write to";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";

  public final String kafkaTopic;
  public final String batchSize;

  public PinotSourceConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.kafkaTopic = this.getString(TOPIC_CONFIG);
    this.batchSize = this.getString(BATCH_SIZE_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
            .define(
                ConfigKeyBuilder.of(TOPIC_CONFIG, Type.STRING)
                        .documentation(TOPIC_DOC)
                        .importance(Importance.HIGH)
                        .build())
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE_CONFIG, Type.STRING)
                        .documentation(BATCH_SIZE_DOC)
                        .importance(Importance.LOW)
                        .build()
        );
  }
}
