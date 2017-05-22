package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class ListMapper extends Mapper<Text, BytesWritable, Text, NullWritable>{

    @Override
        protected void map(Text key, BytesWritable value, Context context)
        throws IOException, InterruptedException{

            context.write(key, NullWritable.get());
        }
}
