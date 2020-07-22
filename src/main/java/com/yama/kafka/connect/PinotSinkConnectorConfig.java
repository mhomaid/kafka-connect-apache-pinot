package com.yama.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class PinotSinkConnectorConfig extends AbstractConfig {

    public static final String PINOT_ZOOKEEPER_CONFIG = "pinot.zookeeper.nodes";
    private static final String PINOT_ZOOKEEPER_DOC = "List of Pinot Zookeepers";

    public static final String PINOT_CLUSTER_NAME_CONFIG = "pinot.cluster.name";
    private static final String PINOT_CLUSTER_NAME_CONFIG_DOC = "Pinot Cluster Name";

    public static final String PINOT_BROKERS_CONFIG = "pinot.brokers.nodes";
    private static final String PINOT_BROKERS_CONFIG_DOC = "List of Pinot Brokers";

    public static final String PINOT_SCHEMA_PATH_CONFIG = "pinot.schema.path";
    private static final String PINOT_SCHEMA_PATH_CONFIG_DOC = "Pinot Schema Path";

    public static final String PINOT_TABLE_NAME_CONFIG = "pinot.table.name";
    private static final String PINOT_TABLE_NAME_CONFIG_DOC = "Pinot Table Name";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";

    public static final String CONNECTOR_CLUSTER_CONFIGS = "Pinot Cluster Configs";
    public static final String CONNECTOR_SCHEMA_CONFIGS = "Pinot Schema Configs";


    public final String batchSize;
    public final String pinotZookeepers;
    public final String pinotBrokers;
    public final String pinotClusterName;
    public final String pinotSchemaPath;
    public final String pinotTableName;

    public PinotSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.batchSize = this.getString(BATCH_SIZE_CONFIG);
        this.pinotZookeepers = this.getString(PINOT_ZOOKEEPER_CONFIG);
        this.pinotBrokers = this.getString(PINOT_BROKERS_CONFIG);
        this.pinotClusterName = this.getString(PINOT_CLUSTER_NAME_CONFIG);
        this.pinotSchemaPath = this.getString(PINOT_SCHEMA_PATH_CONFIG);
        this.pinotTableName = this.getString(PINOT_TABLE_NAME_CONFIG);
    }

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();
        addPinotClusterOptions(config);
        addPinotSchemaOptions(config);
        return config;
    }

    private static void addPinotClusterOptions(ConfigDef config) {
        int orderInGroup = 0;
        config
                .define(
                        ConfigKeyBuilder.of(PINOT_ZOOKEEPER_CONFIG, Type.STRING)
                                .documentation(PINOT_ZOOKEEPER_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_CLUSTER_CONFIGS)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_CLUSTER_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_CLUSTER_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_CLUSTER_CONFIGS)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_BROKERS_CONFIG, Type.STRING)
                                .documentation(PINOT_BROKERS_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_CLUSTER_CONFIGS)
                                .build())
        ;
    }

    private static void addPinotSchemaOptions(ConfigDef config) {
        int orderInGroup = 0;
        config
                .define(
                        ConfigKeyBuilder.of(PINOT_SCHEMA_PATH_CONFIG, Type.STRING)
                                .documentation(PINOT_SCHEMA_PATH_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_CONFIGS)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_TABLE_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_TABLE_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_CONFIGS)
                                .build())
                .define(
                        ConfigKeyBuilder.of(BATCH_SIZE_CONFIG, Type.STRING)
                                .documentation(BATCH_SIZE_DOC)
                                .importance(Importance.LOW)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_CONFIGS)
                                .build()
                );
    }
}
