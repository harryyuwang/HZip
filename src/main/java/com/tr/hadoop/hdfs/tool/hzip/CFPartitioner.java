package com.tr.hzip;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class CFPartitioner extends Partitioner<FileLineWritable, Text> {

    @Override
        public int getPartition(FileLineWritable key, Text value,
                int numReduceTasks) {

            return (key.fileName.hashCode() % numReduceTasks);
        }
}

