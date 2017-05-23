# HZip (HDFS Zip)
## A tool to zip/unzip files on HDFS.

It is well known that Hadoop performs better with a small number of large files, as opposed to a huge number of small files. ("Small" here means significantly smaller than a HDFS block, default to 64MB). So storing huge amount of small files on HDFS does cause harm to the overall performance of your systems, not only by eating up the namenode's memory but also generating excessive amount of mapper tasks which imposes extra bookkeeping overhead. (for more details, please check out [The Small Files Problem - Cloudera Blog](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/))

In order to alleviate the overhead caused by small files on HDFS, I wrote this HDFS zip/unzip utility, which uses Hadoop SequenceFile as a container for large number of small files, with the key being simply the HDFS path and value being the actual compressed content from orginal files.

## Getting Started
You can clone the repo with the following command:

```
$ git clone https://github.com/harryyuwang/HZip.git
``` 

Once you've cloned the repository, build and install the package into your local Maven repo:

```
$ mvn clean install
```

## Usage
```
$ hadoop jar <HZip jar> <Operation> <HDFS Path>

Operations: zip, unzip, list
``` 

## Other Options
Besides Hadoop configuration options, HZip also defines the following specific ones:

```
- hzip.delete:      whether to delete original files/folders after zip/unzip

- hzip.combine:     whether to use CombineFileInputFormat to merge files into fewer input splits.

- hzip.compression: what compression codec to use on SequenceFile record, 
                    supported codec are: gzip, snappy and bzip2
``` 

## Samples
- **Zip**: merge and compress small files into a hadoop sequence file, with an extension of ".hz"

```
// Note: in most cases you may also need to provide hadoop configuration options, such as
// -Dmapred.job.queue.name - Yarn resource pool
// -Dmapred.reduce.tasks - number of reducer tasks

// zip files in a specified folder
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar zip hdfs:///user/hadoop/smallFiles

// zip files and delete original ones after it succeeds
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar -Dhzip.delete=true zip hdfs:///user/hadoop/smallFiles

// zip files using combine input format (to reduce the number of mapper tasks)
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar -Dhzip.delete=true -Dhzip.combine=true zip hdfs:///user/hadoop/smallFiles

// zip files with compression codec
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar -Dhzip.delete=true -Dhzip.combine=true -Dhzip.compression=snappy zip hdfs:///user/hadoop/smallFiles
``` 

- **List**: show a list of files contained in a particular .hz sequence file

```
// list files contained in a sequence file
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar list hdfs:///user/hadoop/smallFiles.hz
``` 

- **unzip**: restore original small files from a sequence file

```
// unzip a sequence file
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar unzip hdfs:///user/hadoop/smallFiles.hz

// unzip a sequence file and delete it after it succeeds
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar -Dhzip.delete=true unzip hdfs:///user/hadoop/smallFiles.hz
``` 

## Combined Splits
In order to make the zip operation work efficiently with huge number of small files on HDFS, option *hzip.combine* is provided which tells the mapreduce job to use a customized CombineFileInputFormat class to combine blocks from different files on the same data node to form a single input split. As a result, it will dramastically reduce the number of mapper tasks required to launch, so very useful if the amount of small files being zipped is really huge.

For instance, if running a zip command with *hzip.combine* set on 4 small files under the HDFS folder *smallFiles*, like:

```
$ hadoop jar HZip-0.2.3-SNAPSHOT.jar -Dhzip.combine=true zip hdfs:///user/hadoop/smallFiles
```
From the log we can see, instead of launching 4 mapper tasks for 4 input splits (the default behavior in FileInputFormat), it actually combine blocks into a single input split, and then launch a single mapper tasks to do the works.

```
17/05/22 23:46:39 INFO input.FileInputFormat: Total input paths to process : 4
17/05/22 23:46:39 INFO input.CombineFileInputFormat: DEBUG: Terminated node allocation with : CompletedNodes: 9, size left: 3666
17/05/22 23:46:39 INFO mapreduce.JobSubmitter: number of splits:1
```

## Contact
If you have any request with regard to HZip you can contact me at harry.wang@thomsonreuters.com. If you would like to contribute with code please check the repo at github: https://github.com/harryyuwang/HZip.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
