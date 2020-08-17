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
    private static final ObjectMapper JSON_SERDE = new ObjectMapper();
    public static final String FILE_NAME_PATTERN = "yyyyMMddHHmmss";
    int batchSize = 100;
    int recordsSize = 0;
    private static final long flushTimeoutMs = 1000;

    private PrintStream _outputStream;

    public PinotRecordsWriter(PrintStream outputStream) {
        _outputStream = outputStream;
    }

    public void initWriter(PinotSinkConnectorConfig config) {
        String topic = "transcript";
        Path segmentsPath = Paths.get(config.getString(PinotSinkConnectorConfig.PINOT_INPUT_DIR_URI_CONFIG) + topic);
        String time = new SimpleDateFormat(FILE_NAME_PATTERN).format(new Date());
        String filename = time + ".json";
        if (!Files.exists(segmentsPath)) {
            try {
                Files.createDirectory(segmentsPath);
                _outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(segmentsPath + filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                        false,
                        StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                throw new ConnectException("Couldn't find or create file '" + filename + "' for PinotSinkSinkTask", e);
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
        String record = null;
        try {

            record = convertRecordToJson(sinkRecord);
            _outputStream.println(record);
            if (recordsSize > batchSize) {
                recordsSize = 0;
            }
            ++recordsSize;
            LOGGER.info("recordsSize {} ", recordsSize);
        } catch (Exception e) {

        }
        if (record != null) {
            LOGGER.trace(
                    "Adding record from topic/partition/offset {}/{}/{} to bulk processor",
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset()
            );
//            bulkProcessor.add(record, sinkRecord, flushTimeoutMs);
        }
    }

    private String convertRecordToJson(SinkRecord sinkRecord) {
        String dataJson;
        try {
            dataJson = JSON_SERDE.writeValueAsString(sinkRecord.value());
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
