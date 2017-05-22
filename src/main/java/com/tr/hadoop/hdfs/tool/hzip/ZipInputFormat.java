package com.tr.hzip;

import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.Path;


public class ZipInputFormat extends FileInputFormat<NullWritable,BytesWritable>{

    @Override
        protected boolean isSplitable(JobContext context,Path file){
            return false;
        }

    @Override
        public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException,InterruptedException{
            ZipRecordReader reader = new ZipRecordReader();
            reader.initialize(split,context);
            return reader;
        }
}

