package com.yama.kafka.connect.pinot.batch;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkProcessor {

    private static final Logger log = LoggerFactory.getLogger(BulkProcessor.class);
    private final Time time;
    private final int maxBufferedRecords;
    private final int batchSize;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;

    public BulkProcessor(Time time, int maxBufferedRecords, int batchSize, long lingerMs, int maxRetries, long retryBackoffMs) {
        this.time = time;
        this.maxBufferedRecords = maxBufferedRecords;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }
}
