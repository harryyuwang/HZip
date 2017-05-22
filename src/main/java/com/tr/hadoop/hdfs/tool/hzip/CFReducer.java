package com.tr.hzip;

import java.io.*;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;

public class CFReducer extends Reducer<FileLineWritable, Text, Text, BytesWritable> {

    public void reduce(FileLineWritable key, Iterable<Text> values, Context context
            ) throws IOException, InterruptedException {

        String combinedValues = new String();
        for (Text value : values) {
            if (!combinedValues.isEmpty()) {
                combinedValues += "\n";
            }
            combinedValues += value;
        }
        combinedValues += "\n";

        Text combinedText = new Text(combinedValues);
        System.out.println(key.toString() + combinedValues);
        context.write(new Text(key.fileName), new BytesWritable(combinedText.getBytes()));
    }
}
