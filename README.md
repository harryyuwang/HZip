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

- hzip.combine:     whether to use CombineFileInputFormat to merge files into fewer input split.

- hzip.compression: what compression codec to use on SequenceFile record, 
                    supported codec are: gzip, snappy and bzip2
``` 
