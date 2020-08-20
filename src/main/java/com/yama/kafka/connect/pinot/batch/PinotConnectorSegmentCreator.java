package com.yama.kafka.connect.pinot.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yama.kafka.connect.PinotSinkConnectorConfig;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class PinotConnectorSegmentCreator {
    private static Logger LOGGER = LoggerFactory.getLogger(PinotConnectorSegmentCreator.class);

    public static final String JSON = ".json";
    private static final double MAX_VALUE = Integer.MAX_VALUE;
    private static final int NUM_ROWS = 1000;
    public static IndexSegment _indexSegment;
    private List<IndexSegment> _indexSegments;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    PinotSinkConnectorConfig _config;


    public void generateSegment(PinotSinkConnectorConfig config)
            throws Exception {
        LOGGER.info("generateSegment ");
        String TABLE_NAME = config.getString(PinotSinkConnectorConfig.PINOT_TABLE_NAME_CONFIG);
        String SEGMENT_NAME = config.getString(PinotSinkConnectorConfig.PINOT_TABLE_NAME_CONFIG);
        String RECORDS_FILES_PATH = config.getString(PinotSinkConnectorConfig.PINOT_INPUT_DIR_URI_CONFIG);
        String SEGMENT_FILES_PATH = config.getString(PinotSinkConnectorConfig.OUTPUT_DIR_URI_CONFIG);
        File INDEX_DIR = new File(SEGMENT_FILES_PATH);

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(
                new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build(),
                buildSchemaFromFile());

        segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
        segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
        segmentGeneratorConfig.setTableName(TABLE_NAME);
        String file = RECORDS_FILES_PATH + TABLE_NAME + JSON;

        List<GenericRow> segmentRecords = new ArrayList<>();
        GenericRow segmentRecord = new GenericRow();
        JsonNode jsonNode;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String json;
            while ((json = br.readLine()) != null) {
                jsonNode = OBJECT_MAPPER.readTree(json);
                Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    segmentRecord.putValue(field.getKey(), field.getValue().textValue());
                }
                segmentRecords.add(segmentRecord);
            }
        }

        // Build the segment
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(segmentRecords));
        driver.build();

        // Tar the segment -> 0.4.0 API
        TarGzCompressionUtils.createTarGzOfDirectory(INDEX_DIR.getAbsolutePath(), SEGMENT_FILES_PATH + SEGMENT_NAME);
        // Tar the segment -> 0.5.0 API
        LOGGER.info("Created the segment her : {} ", SEGMENT_FILES_PATH + SEGMENT_NAME);
    }

    /*
    private static void createTarGzFile(SegmentIndexCreationDriverImpl driver) {
        String segmentName = driver.getSegmentName();
        File indexDir = new File(INDEX_DIR.getAbsolutePath(), segmentName);
        File segmentTarFile = new File(SEGMENT_FILES_PATH, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);

    }
    */


    private Schema buildSchemaFromFile() {
        Schema schema = new Schema();
        String schemaPath = _config.getString(PinotSinkConnectorConfig.PINOT_SCHEMA_PATH_CONFIG);
        String schemaName = _config.getString(PinotSinkConnectorConfig.PINOT_SCHEMA_NAME_CONFIG);
        LOGGER.info("buildSchemaFromFile {} ", schemaPath);
        if (schemaPath == null) {
            LOGGER.error("No schema is needs to be there ");
        }
        try {
            return Schema.fromFile(new File(schemaPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }
}
