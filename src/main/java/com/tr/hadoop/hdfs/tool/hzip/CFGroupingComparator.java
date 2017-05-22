package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class CFGroupingComparator extends WritableComparator {
    protected CFGroupingComparator() {
        super(FileLineWritable.class, true);
    }

    @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            FileLineWritable key1 = (FileLineWritable) w1;
            FileLineWritable key2 = (FileLineWritable) w2;
            return key1.fileName.compareTo(key2.fileName);
        }
}
