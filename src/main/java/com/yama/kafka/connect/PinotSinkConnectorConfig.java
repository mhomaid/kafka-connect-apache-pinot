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

    public static final String PINOT_SCHEMA_NAME_CONFIG = "pinot.schema.name";
    private static final String PINOT_SCHEMA_NAME_CONFIG_DOC = "Pinot Schema Name";

    public static final String PINOT_TABLE_NAME_CONFIG = "pinot.table.name";
    private static final String PINOT_TABLE_NAME_CONFIG_DOC = "Pinot Table Name";

    public static final String PINOT_JOB_TYPE_CONFIG = "job.type";
    private static final String PINOT_JOB_TYPE_CONFIG_DOC = "Job Type";

    public static final String PINOT_INPUT_DIR_URI_CONFIG = "input.dir.uri";
    private static final String PINOT_INPUT_DIR_URI_CONFIG_DOC = "Input Dir URI";

    public static final String INCLUDE_FILE_NAME_PATTERN_CONFIG = "include.file.name.pattern";
    private static final String INCLUDE_FILE_NAME_PATTERN_CONFIG_DOC = "Include File Name Pattern";

    public static final String OUTPUT_DIR_URI_CONFIG = "output.dir.uri";
    private static final String OUTPUT_DIR_URI_CONFIG_DOC = "";

    public static final String OVERWRITE_OUTPUT_CONFIG = "overwrite.output";
    private static final String OVERWRITE_OUTPUT_CONFIG_DOC = "";

    public static final String PINOT_FS_SPECS_SCHEMA_CONFIG = "pinot.fs.specs.schema";
    private static final String PINOT_FS_SPECS_SCHEMA_CONFIG_DOC = "Include File Name Pattern";

    public static final String PINOT_FS_SPECS_CLASS_NAME_CONFIG = "pinot.fs.specs.class.name";
    public static final String PINOT_FS_SPECS_CLASS_NAME_CONFIG_DOC = "pinot.fs.specs.class.name";

    public static final String RECORD_READER_SPEC_DATA_FORMAT_CONFIG = "recordReaderSpec.dataFormat";
    private static final String RECORD_READER_SPEC_DATA_FORMAT_CONFIG_DOC = "Include File Name Pattern";

    public static final String RECORD_READER_SPEC_CLASS_NAME_CONFIG = "recordReaderSpec.className";

    public static final String RECORD_READER_SPEC_CONFIG_CLASS_NAME_CONFIG = "recordReaderSpec.configClassName";

    public static final String TABLE_SPEC_TABLE_NAME_CONFIG = "tableSpec.tableName";

    public static final String PINOT_CLUSTER_SPECS_CONTROLLER_URI_CONFIG = "pinotClusterSpecs.controllerURI";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";

    /*
        Stream Configs
     */
    public static final String STREAM_FETCH_TIMEOUT_MILLIS = "fetch.timeout.millis";
    /**
     * Time threshold that will keep the realtime segment open for before we complete the segment
     */
    public static final String SEGMENT_FLUSH_THRESHOLD_TIME = "realtime.segment.flush.threshold.time";

    public static final String SEGMENT_FLUSH_THRESHOLD_ROWS = "realtime.segment.flush.threshold.size";
    public static final String SEGMENT_FLUSH_DESIRED_SIZE = "realtime.segment.flush.desired.size";
    /**
     * The initial num rows to use for segment size auto tuning. By default 100_000 is used.
     */
    public static final String SEGMENT_FLUSH_AUTO_TUNE_INITIAL_ROWS = "realtime.segment.flush.autotune.initialRows";

    // Time threshold that controller will wait for the segment to be built by the server
    public static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "realtime.segment.commit.timeoutSeconds";

    public static final String CONNECTOR_CLUSTER_GROUP = "Pinot Cluster Configs";
    public static final String CONNECTOR_SCHEMA_GROUP = "Pinot Schema Configs";
    public static final String CONNECTOR_BATCH_SPEC_GROUP = "Pinot Batch Spec Configs";
    public static final String CONNECTOR_STREAM_GROUP = "Pinot Stream Spec Configs";


    public final String batchSize;
    public final String pinotZookeepers;
    public final String pinotBrokers;
    public final String pinotClusterName;
    public final String pinotSchemaPath;
    public final String pinotTableName;
    public final String outputDirURI;
    public final String schemaName;

    public PinotSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.batchSize = this.getString(BATCH_SIZE_CONFIG);
        this.pinotZookeepers = this.getString(PINOT_ZOOKEEPER_CONFIG);
        this.pinotBrokers = this.getString(PINOT_BROKERS_CONFIG);
        this.pinotClusterName = this.getString(PINOT_CLUSTER_NAME_CONFIG);
        this.pinotSchemaPath = this.getString(PINOT_SCHEMA_PATH_CONFIG);
        this.pinotTableName = this.getString(PINOT_TABLE_NAME_CONFIG);
        this.outputDirURI = this.getString(OUTPUT_DIR_URI_CONFIG);
        this.schemaName = this.getString(PINOT_SCHEMA_NAME_CONFIG);
    }

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();
        addPinotClusterOptions(config);
        addPinotSchemaOptions(config);
        addPinotBatchJobOptions(config);
        addPinotStreamOptions(config);
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
                                .group(CONNECTOR_CLUSTER_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_CLUSTER_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_CLUSTER_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_CLUSTER_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_BROKERS_CONFIG, Type.STRING)
                                .documentation(PINOT_BROKERS_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_CLUSTER_GROUP)
                                .build())
        ;
    }


    private static void addPinotBatchJobOptions(ConfigDef config) {
        int orderInGroup = 1;
        config
                .define(
                        ConfigKeyBuilder.of(PINOT_JOB_TYPE_CONFIG, Type.STRING)
                                .documentation(PINOT_JOB_TYPE_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("SegmentCreationAndTarPush")
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_INPUT_DIR_URI_CONFIG, Type.STRING)
                                .documentation(PINOT_INPUT_DIR_URI_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("")
                                .build())
                .define(
                        ConfigKeyBuilder.of(INCLUDE_FILE_NAME_PATTERN_CONFIG, Type.STRING)
                                .documentation(INCLUDE_FILE_NAME_PATTERN_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("glob:**/*.json")
                                .build())
                .define(
                        ConfigKeyBuilder.of(OUTPUT_DIR_URI_CONFIG, Type.STRING)
                                .documentation(OUTPUT_DIR_URI_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("")
                                .build())
                .define(
                        ConfigKeyBuilder.of(OVERWRITE_OUTPUT_CONFIG, Type.BOOLEAN)
                                .documentation(OVERWRITE_OUTPUT_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue(true)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_FS_SPECS_SCHEMA_CONFIG, Type.STRING)
                                .documentation(PINOT_FS_SPECS_SCHEMA_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("file")
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_FS_SPECS_CLASS_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_FS_SPECS_CLASS_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("org.apache.pinot.spi.filesystem.LocalPinotFS")
                                .build())

                .define(
                        ConfigKeyBuilder.of(RECORD_READER_SPEC_DATA_FORMAT_CONFIG, Type.STRING)
                                .documentation(RECORD_READER_SPEC_DATA_FORMAT_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("json")
                                .build())
                .define(
                        ConfigKeyBuilder.of(RECORD_READER_SPEC_CLASS_NAME_CONFIG, Type.STRING)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("org.apache.pinot.plugin.inputformat.json.JsonRecordReader")
                                .build())
                .define(
                        ConfigKeyBuilder.of(RECORD_READER_SPEC_CONFIG_CLASS_NAME_CONFIG, Type.STRING)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .defaultValue("org.apache.pinot.plugin.inputformat.json.JSONRecordReaderConfig")
                                .build())
                .define(
                        ConfigKeyBuilder.of(TABLE_SPEC_TABLE_NAME_CONFIG, Type.STRING)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_CLUSTER_SPECS_CONTROLLER_URI_CONFIG, Type.STRING)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_BATCH_SPEC_GROUP)
                                .build())
        ;
    }

    private static void addPinotSchemaOptions(ConfigDef config) {
        int orderInGroup = 2;
        config
                .define(
                        ConfigKeyBuilder.of(PINOT_SCHEMA_PATH_CONFIG, Type.STRING)
                                .documentation(PINOT_SCHEMA_PATH_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_SCHEMA_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_SCHEMA_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(PINOT_TABLE_NAME_CONFIG, Type.STRING)
                                .documentation(PINOT_TABLE_NAME_CONFIG_DOC)
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_GROUP)
                                .build())
                .define(
                        ConfigKeyBuilder.of(BATCH_SIZE_CONFIG, Type.STRING)
                                .documentation(BATCH_SIZE_DOC)
                                .importance(Importance.LOW)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_SCHEMA_GROUP)
                                .build()
                );
    }

    private static void addPinotStreamOptions(ConfigDef config) {
        int orderInGroup = 3;
        config
                .define(
                        ConfigKeyBuilder.of(STREAM_FETCH_TIMEOUT_MILLIS, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(30_000)
                                .build())
                .define(
                        ConfigKeyBuilder.of(SEGMENT_FLUSH_THRESHOLD_TIME, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(5_000)
                                .build())
                .define(
                        ConfigKeyBuilder.of(SEGMENT_FLUSH_THRESHOLD_ROWS, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(100_000)
                                .build())
                .define(
                        ConfigKeyBuilder.of(SEGMENT_FLUSH_DESIRED_SIZE, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(100_000)
                                .build())
                .define(
                        ConfigKeyBuilder.of(SEGMENT_FLUSH_AUTO_TUNE_INITIAL_ROWS, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(100_000)
                                .build())
                .define(
                        ConfigKeyBuilder.of(SEGMENT_COMMIT_TIMEOUT_SECONDS, Type.LONG)
                                .documentation("")
                                .importance(Importance.HIGH)
                                .orderInGroup(++orderInGroup)
                                .group(CONNECTOR_STREAM_GROUP)
                                .defaultValue(100_000)
                                .build())
        ;
    }


}
