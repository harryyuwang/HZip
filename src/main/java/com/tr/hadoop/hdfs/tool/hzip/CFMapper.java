package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;


public class CFMapper extends Mapper<FileLineWritable, Text, FileLineWritable, Text>{

    public void map (FileLineWritable key, Text val, Context context) throws IOException, InterruptedException{
        // context.write(new Text(key.fileName), new Text(Long.toString(key.offset) + "|" + val));
        context.write(key, val);
    }
}
