package com.tr.hadoop.hdfs.tool;

import java.io.*;
import java.net.URI;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tr.hadoop.hdfs.tool.hzip.*;

public class HZip extends Configured implements Tool {

    private static final String LISTINFO_PARTONE = "part-r-00000";
    private static final String DEFAULT_POOL =  "root.default";
    private static final String DEFAULT_COMPRESSION = "none";

    private static Configuration conf = new Configuration();
    private static Logger logger = LoggerFactory.getLogger(HZip.class);

    public int run(String[] args) throws Exception {
        logger.info("operation about to perform: " + args[0]);
        logger.info("running in pool:            " + conf.get("mapred.job.queue.name", DEFAULT_POOL));
        logger.info("delete flag:                " + conf.getBoolean("hzip.delete", false));
        logger.info("input combine flag:         " + conf.getBoolean("hzip.combine", false));
        logger.info("compression option:         " + conf.get("hzip.compression", DEFAULT_COMPRESSION));

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if (!(fs.exists(path))) {
            logger.error("hdfs file:" + args[1] + " doesn't exists!");
            return 2;
        }

        Job job = null;
        if (args[0].equals("zip")) {
            conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

            job = new Job(conf,"HZip - zip");
            job.setJarByClass(HZip.class);
            // FileInputFormat.addInputPath(job, path);
            // FileOutputFormat.setOutputPath(job, new Path(args[1] + ".hz"));

            String compressOpt = conf.get("hzip.compression", DEFAULT_COMPRESSION);
            if (compressOpt.equals(DEFAULT_COMPRESSION)) {
                // do nothing
            }
            else if (compressOpt.equals("gzip")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            }
            else if (compressOpt.equals("snappy")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            }
            else if (compressOpt.equals("bzip2")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
            }
            else {
                logger.error("incorrect compression option: " + compressOpt);
                return 2;
            }
    
            if (!conf.getBoolean("hzip.combine", false)) {
                job.setMapperClass(ZipMapper.class);
                job.setInputFormatClass(ZipInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);

                FileInputFormat.addInputPath(job, path);
                FileOutputFormat.setOutputPath(job, new Path(args[1] + ".hz"));

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(BytesWritable.class);
            }
            else {
                job.setMapperClass(CFMapper.class);
                job.setMapOutputKeyClass(FileLineWritable.class);
                job.setMapOutputValueClass(Text.class);            

                job.setPartitionerClass(CFPartitioner.class);
                job.setSortComparatorClass(CFSortComparator.class);
                job.setGroupingComparatorClass(CFGroupingComparator.class);

                job.setInputFormatClass(CFInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);

                job.setReducerClass(CFReducer.class);
                FileInputFormat.addInputPath(job, path);
                FileOutputFormat.setOutputPath(job, new Path(args[1] + ".hz"));

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(BytesWritable.class);
            }
        } 
        else if (args[0].equals("unzip")) {
            job = new Job(conf,"HZip - unzip");
            job.setJarByClass(HZip.class);
            job.setMapperClass(UnzipMapper.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
        
            FileInputFormat.addInputPath(job, path);
        }
        else if (args[0].equals("list")) {
            conf.setBoolean("hzip.delete", false);
            
            if (FilenameUtils.getExtension(args[1]).equals("hz")) {
                job = new Job(conf,"HZip - list");
                job.setJarByClass(HZip.class);
                job.setMapperClass(ListMapper.class);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setNumReduceTasks(1);

                FileInputFormat.addInputPath(job, path);
                FileOutputFormat.setOutputPath(job, new Path(args[1] + ".info"));

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

            }
            else {
                logger.error(args[1] + " is not a HZipped file!");
                return 2;
            }
        }
        else {
            logger.error("incorrect operation: " + args[0]);
            return 2;
        }

        if (conf.getBoolean("hzip.delete", false)) {        
            return (job.waitForCompletion(true) && fs.delete(path, true))? 0:1;
        }
        else {
            int ret = job.waitForCompletion(true)?0:1;

            if (args[0].equals("list")) {
                try{
                    BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(new Path(args[1] + ".info/" + LISTINFO_PARTONE))));

                    logger.info("HZipped file " + args[1] + " contains:");
                    String line; int num = 1;
                    line=br.readLine();
                    while (line != null){
                        logger.info("  => [" + num + "]\t" + line);
                        line=br.readLine();
                        num++;
                    }

                    fs.delete(new Path(args[1] + ".info"), true);
                }
                catch(Exception e) {
                    ret = 1;
                    e.printStackTrace();
                }
            } 

            return ret;
        }
    }

    public static void main( String[] args ) throws Exception {
        String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (programArgs.length != 2) {
               logger.error("Usage: HZip [Operations] HDFS_Path");
               logger.error("\tOperations: zip, unzip, list");
               System.exit(2);
        }

        int exitcode = ToolRunner.run(new HZip(), programArgs);
        System.exit(exitcode);
    }
}
