package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CFSortComparator extends WritableComparator {

    protected CFSortComparator() {
        super(FileLineWritable.class, true);
    }

    @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            FileLineWritable key1 = (FileLineWritable) w1;
            FileLineWritable key2 = (FileLineWritable) w2;

            int cmp = key1.fileName.compareTo(key2.fileName);
            if (cmp != 0) return cmp;

            return (int)Math.signum((double)(key1.offset - key2.offset));
        }
}
