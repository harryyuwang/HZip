package com.tr.hadoop.hdfs.tool.hzip;

import java.io.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;


public class UnzipMapper extends Mapper<Text, BytesWritable, Text, Text>{

    private static FileSystem hdfs = null;
    private static Configuration conf = null;

    @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration(); 
            hdfs = FileSystem.get(conf);
        }

    @Override
        protected void map(Text key, BytesWritable value, Context context)
        throws IOException, InterruptedException{

            Path outputPath = new Path(key.toString());
            OutputStream os = hdfs.create(outputPath);


            value.setCapacity(value.getLength());

            byte[] content = value.getBytes();

            // This is a crappy hack, trying to remove the tailing ^@ characters at
            // the end of each files. I haven't figured out where those wierd
            // characters come from though.
            int index = 0;
            for (byte b : content) {
                if (Byte.toString(b).equals("0")) {
                    content = ArrayUtils.remove(content, index);
                }
                else {
                    index++;
                }
            }

            InputStream is = new ByteArrayInputStream(content);
            IOUtils.copyBytes(is, os, Long.valueOf(value.getLength()), true);
        }
}
