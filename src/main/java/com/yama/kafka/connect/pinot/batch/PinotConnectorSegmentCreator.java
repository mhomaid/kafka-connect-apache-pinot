package com.yama.kafka.connect.pinot.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class PinotConnectorSegmentCreator {
    private static Logger LOGGER = LoggerFactory.getLogger(PinotConnectorSegmentCreator.class);
    public static final String STUDENT_ID = "studentID";
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String GENDER = "gender";
    public static final String SUBJECT = "subject";
    public static final String SCORE = "score";

    private static final String SEGMENT_FILES_PATH = "/Users/mohamed.homaid/Apache/Pinot/demo-pinot/segments/1";
    private static final File INDEX_DIR = new File(SEGMENT_FILES_PATH);
    private static final String SEGMENT_NAME = "Transcript_Segment";
    private static final String TABLE_NAME = "transcript";
    private static final double MAX_VALUE = Integer.MAX_VALUE;
    private static final int NUM_ROWS = 1000;
    public static IndexSegment _indexSegment;
    private List<IndexSegment> _indexSegments;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        try {
            buildSegment();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void buildSegment()
            throws Exception {

        Schema schema = buildSchema();
        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
        JsonNode jsonNode;
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
        config.setSegmentName(SEGMENT_NAME);
        config.setOutDir(INDEX_DIR.getAbsolutePath());
        config.setTableName(TABLE_NAME);
        String file = SEGMENT_FILES_PATH + TABLE_NAME;


        List<GenericRow> segmentRecords = new ArrayList<>();
        GenericRow segmentRecord = new GenericRow();
        String json;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            while ((json = br.readLine()) != null) {
                jsonNode = OBJECT_MAPPER.readTree(json);
                segmentRecord.putValue(STUDENT_ID, jsonNode.get(STUDENT_ID).asText());
                segmentRecord.putValue(FIRST_NAME, jsonNode.get(FIRST_NAME).asText());
                segmentRecord.putValue(LAST_NAME, jsonNode.get(LAST_NAME).asText());
                segmentRecord.putValue(GENDER, jsonNode.get(GENDER).asText());
                segmentRecord.putValue(SUBJECT, jsonNode.get(SUBJECT).asText());
                segmentRecord.putValue(SCORE, jsonNode.get(SCORE).asText());
                segmentRecords.add(segmentRecord);
            }
        }


        // Build the segment
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(config, new GenericRowRecordReader(segmentRecords));
        driver.build();

        // Tar the segment
//        createTarGzFile(driver);
        TarGzCompressionUtils.createTarGzOfDirectory(INDEX_DIR.getAbsolutePath(), SEGMENT_FILES_PATH);
    }

    private static void createTarGzFile(SegmentIndexCreationDriverImpl driver) {
        String segmentName = driver.getSegmentName();
        File indexDir = new File(INDEX_DIR.getAbsolutePath(), segmentName);
        File segmentTarFile = new File(SEGMENT_FILES_PATH, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
//        TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);
    }


    private static Schema buildSchema() {
        Schema schema = new Schema();

        schema.addField(new DimensionFieldSpec(STUDENT_ID, FieldSpec.DataType.INT, true));
        schema.addField(new DimensionFieldSpec(FIRST_NAME, FieldSpec.DataType.STRING, true));
        schema.addField(new DimensionFieldSpec(LAST_NAME, FieldSpec.DataType.STRING, true));
        schema.addField(new DimensionFieldSpec(GENDER, FieldSpec.DataType.STRING, true));
        schema.addField(new DimensionFieldSpec(SUBJECT, FieldSpec.DataType.STRING, true));
        schema.addField(new MetricFieldSpec(SCORE, FieldSpec.DataType.FLOAT));
        return schema;
    }

}
