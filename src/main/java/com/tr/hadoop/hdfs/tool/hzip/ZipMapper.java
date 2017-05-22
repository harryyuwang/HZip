package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;


public class ZipMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

    private Text fileName;

    @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            fileName = new Text(path.toString());
        }

    @Override
        protected void map(NullWritable key, BytesWritable value,Context context)
        throws IOException, InterruptedException{
            // System.out.println(fileName.toString() + (new String(value.getBytes())));
            context.write(fileName, value);
        }
}
