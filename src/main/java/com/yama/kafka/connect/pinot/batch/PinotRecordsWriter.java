package com.yama.kafka.connect.pinot.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yama.kafka.connect.PinotSinkConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

public class PinotRecordsWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PinotRecordsWriter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String FILE_NAME_PATTERN = "yyyyMMddHHmmss";
    public static final String JSON = ".json";
    int batchSize = 100;
    int recordsSize = 0;
    private static final long flushTimeoutMs = 1000;

    private PrintStream _outputStream;
    private final PinotConnectorSegmentCreator _pinotSegmentCreator;
    private PinotSinkConnectorConfig _config;

    public PinotRecordsWriter(PrintStream outputStream, PinotConnectorSegmentCreator pinotSegmentCreator) {
        _outputStream = outputStream;
        _pinotSegmentCreator = pinotSegmentCreator;
    }

    public void initWriter(PinotSinkConnectorConfig config) {
        _config = config;
        batchSize = Integer.parseInt(config.getString(PinotSinkConnectorConfig.BATCH_SIZE_CONFIG));
        String rawDataPath = config.getString(PinotSinkConnectorConfig.PINOT_INPUT_DIR_URI_CONFIG);
        String file = config.getString(PinotSinkConnectorConfig.PINOT_TABLE_NAME_CONFIG) + JSON;
        String time = new SimpleDateFormat(FILE_NAME_PATTERN).format(new Date());
        createDir(rawDataPath);
        createRawRecordsFile(rawDataPath, file);

    }

    private void createRawRecordsFile(String rawDataPath, String file) {
        try {
            _outputStream = new PrintStream(
                    Files.newOutputStream(Paths.get(rawDataPath + file), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    false,
                    StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new ConnectException("Couldn't find or create file '" + file + "' for PinotSinkSinkTask", e);
        }
    }

    private void createDir(String path) {
        Path _path = Paths.get(path);
        if (!Files.exists(_path)) {
            try {
                Files.createDirectory(_path);
            } catch (IOException e) {
                LOGGER.info("Couldn't find or create Director in {} ", _path);
            }
        }
    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord sinkRecord : records) {
            // Preemptively skip records with null values if they're going to be ignored anyways
            if (ignoreRecord(sinkRecord)) {
                LOGGER.trace(
                        "Ignoring sink record with null value for topic/partition/offset {}/{}/{}",
                        sinkRecord.topic(),
                        sinkRecord.kafkaPartition(),
                        sinkRecord.kafkaOffset()
                );
                continue;
            }
            LOGGER.trace("Writing record raw initial file: topic/partition/offset {}/{}/{}",
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset()
            );
            tryWriteRecord(sinkRecord);
        }
    }

    private void tryWriteRecord(SinkRecord sinkRecord) {
        String record = convertRecordToJson(sinkRecord);
        if (record != null) {
            LOGGER.trace(
                    "Adding record from topic/partition/offset {}/{}/{} to bulk processor",
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset()
            );
            _outputStream.println(record);
//            bulkProcessor.add(record, sinkRecord, flushTimeoutMs);
            if (recordsSize > batchSize) {
                try {
                    _pinotSegmentCreator.generateSegment(_config);
                    LOGGER.info("_pinotSegmentCreator {} ", batchSize);
                    flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                recordsSize = 0;

            }
            ++recordsSize;
        }
    }

    private String convertRecordToJson(SinkRecord sinkRecord) {
        String dataJson;
        try {
            dataJson = OBJECT_MAPPER.writeValueAsString(sinkRecord.value());
        } catch (JsonProcessingException e) {
            dataJson = "Bad data can't be written as json: " + e.getMessage();
        }
        return dataJson;
    }

    private boolean ignoreRecord(SinkRecord record) {
        return record.value() == null;
    }

    public void flush() {
//        bulkProcessor.flush(flushTimeoutMs);
        _outputStream.flush();
    }

    public void start() {
//        bulkProcessor.start();
    }


    public void stop() {
        if (_outputStream != null && _outputStream != System.out)
            _outputStream.close();
    }

}
