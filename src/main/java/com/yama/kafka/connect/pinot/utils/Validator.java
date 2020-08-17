package com.yama.kafka.connect.pinot.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Validator {

    public static void main(String[] args) {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        System.out.printf("fileSuffix " + fileSuffix);
    }
}
