package com.yama.kafka.connect.pinot.batch;

import org.apache.pinot.core.indexsegment.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class PinotConnectorSegmentCreator {

    private static Logger LOGGER = LoggerFactory.getLogger(PinotConnectorSegmentCreator.class);
    private static final String SEGMENT_FILES_PATH = "/Users/mohamed.homaid/Apache/Pinot/kafka-connect-apache-pinot/data/segments";
    private static final File INDEX_DIR = new File(SEGMENT_FILES_PATH);
    private static final String SEGMENT_NAME = "Segment1";

    private static final double MAX_VALUE = Integer.MAX_VALUE;
    private static final int NUM_ROWS = 1000;

    public static IndexSegment _indexSegment;


}
