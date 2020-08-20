package com.yama.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.yama.kafka.connect.pinot.batch.PinotConnectorSegmentCreator;
import com.yama.kafka.connect.pinot.batch.PinotRecordsWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;

public class PinotSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(PinotSinkTask.class);

    PinotSinkConnectorConfig config;
    PinotRecordsWriter writer;
    PrintStream outputStream;
    PinotConnectorSegmentCreator segmentCreator;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Pinot Sink task");
        config = new PinotSinkConnectorConfig(props);
        // Create api connections here.
        initWriter(config);
    }

    public void initWriter(PinotSinkConnectorConfig config) {
        writer = new PinotRecordsWriter(outputStream, segmentCreator);
        writer.initWriter(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        writer.write(records);
        LOGGER.info("Read {} records from Kafka", records.size());

    }


    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        writer.flush();

    }

    @Override
    public void stop() {
        LOGGER.info("Stopping task");
        writer.stop();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        LOGGER.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
