package com.yama.kafka.connect.pinot.stream;

import org.apache.pinot.spi.stream.StreamConfig;

import java.util.Map;

public class PinotStreamWriter {

    String tableNameWithType;
    Map<String, String> streamConfigMap;
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
}
