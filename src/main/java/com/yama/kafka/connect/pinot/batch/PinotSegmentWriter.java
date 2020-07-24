package com.yama.kafka.connect.pinot.batch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yama.kafka.connect.PinotSinkConnectorConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.core.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class PinotSegmentWriter {

    public static final String SIMPLE_SEGMENT_NAME_GENERATOR = "simple";
    public static final String NORMALIZED_DATE_SEGMENT_NAME_GENERATOR = "normalizedDate";

    // For SimpleSegmentNameGenerator
    public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

    // For NormalizedDateSegmentNameGenerator
    public static final String SEGMENT_NAME_PREFIX = "segment.name.prefix";
    public static final String EXCLUDE_SEQUENCE_ID = "exclude.sequence.id";
    PinotSinkConnectorConfig _connectorConfig;

    public void init(PinotSinkConnectorConfig connectorConfig) {
        _connectorConfig = connectorConfig;

    }

    public SegmentGenerationTaskRunner generateSegment() throws Exception {
        SegmentGenerationTaskSpec spec = genSegmentGenerationTaskSpec();
        return new SegmentGenerationTaskRunner(spec);
    }

    private SegmentGenerationTaskSpec genSegmentGenerationTaskSpec() throws Exception {
        SegmentGenerationTaskSpec _taskSpec = new SegmentGenerationTaskSpec();
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        _taskSpec.setInputFilePath(_connectorConfig.pinotSchemaPath);
        _taskSpec.setOutputDirectoryPath(_connectorConfig.outputDirURI);

        TableSpec tableSpec = new TableSpec();


        TableConfig tableConfig = JsonUtils.jsonNodeToObject(_taskSpec.getTableConfig(), TableConfig.class);
        String tableName = tableConfig.getTableName();
        Schema schema = new Schema();
        schema.setSchemaName(_connectorConfig.pinotSchemaPath);
        _taskSpec.setSchema(schema);

        //init record reader config
        RecordReaderSpec recordReaderSpec;

//        _taskSpec.setRecordReaderSpec(recordReaderSpec);
        String readerConfigClassName = _taskSpec.getRecordReaderSpec().getConfigClassName();
        RecordReaderConfig recordReaderConfig = null;

        if (readerConfigClassName != null) {
            Map<String, String> configs = _taskSpec.getRecordReaderSpec().getConfigs();
            if (configs == null) {
                configs = new HashMap<>();
            }
            JsonNode jsonNode = new ObjectMapper().valueToTree(configs);
            Class<?> clazz = PluginManager.get().loadClass(readerConfigClassName);
            recordReaderConfig = (RecordReaderConfig) JsonUtils.jsonNodeToObject(jsonNode, clazz);
        }

        //init segmentName Generator
        SegmentNameGenerator segmentNameGenerator = getSegmentNameGenerator(_taskSpec);

        //init segment generation config
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setTableName(tableName);
        segmentGeneratorConfig.setOutDir(_taskSpec.getOutputDirectoryPath());
        segmentGeneratorConfig.setSegmentNameGenerator(segmentNameGenerator);
        segmentGeneratorConfig.setSequenceId(_taskSpec.getSequenceId());
        segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
        segmentGeneratorConfig.setRecordReaderPath(_taskSpec.getRecordReaderSpec().getClassName());
        segmentGeneratorConfig.setInputFilePath(_taskSpec.getInputFilePath());

        //build segment
        SegmentIndexCreationDriverImpl segmentIndexCreationDriver = new SegmentIndexCreationDriverImpl();
        segmentIndexCreationDriver.init(segmentGeneratorConfig);
        segmentIndexCreationDriver.build();
        segmentIndexCreationDriver.getSegmentName();


        return _taskSpec;
    }

    private static SegmentNameGenerator getSegmentNameGenerator(SegmentGenerationTaskSpec _taskSpec)
            throws IOException {
        TableConfig tableConfig = JsonUtils.jsonNodeToObject(_taskSpec.getTableConfig(), TableConfig.class);
        String tableName = tableConfig.getTableName();

        Schema schema = _taskSpec.getSchema();
        SegmentNameGeneratorSpec segmentNameGeneratorSpec = _taskSpec.getSegmentNameGeneratorSpec();
        if (segmentNameGeneratorSpec == null) {
            segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
        }
        String segmentNameGeneratorType = segmentNameGeneratorSpec.getType();
        if (segmentNameGeneratorType == null) {
            segmentNameGeneratorType = SIMPLE_SEGMENT_NAME_GENERATOR;
        }
        Map<String, String> segmentNameGeneratorConfigs = segmentNameGeneratorSpec.getConfigs();
        if (segmentNameGeneratorConfigs == null) {
            segmentNameGeneratorConfigs = new HashMap<>();
        }
        switch (segmentNameGeneratorType) {
            case SIMPLE_SEGMENT_NAME_GENERATOR:
                return new SimpleSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_POSTFIX));
            case NORMALIZED_DATE_SEGMENT_NAME_GENERATOR:
                SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
                DateTimeFormatSpec dateTimeFormatSpec = null;
                String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();

                if (timeColumnName != null) {
                    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
                    if (dateTimeFieldSpec != null) {
                        dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
                    }
                }
                return new NormalizedDateSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_PREFIX),
                        Boolean.parseBoolean(segmentNameGeneratorConfigs.get(EXCLUDE_SEQUENCE_ID)),
                        validationConfig.getSegmentPushType(), validationConfig.getSegmentPushFrequency(), dateTimeFormatSpec);
            default:
                throw new UnsupportedOperationException("Unsupported segment name generator type: " + segmentNameGeneratorType);
        }
    }
}
