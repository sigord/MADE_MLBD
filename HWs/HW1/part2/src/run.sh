#!/usr/bin/env bash

set -x

HADOOP_STREAMING_JAR=/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar
HDFS_OUTPUT_DIR_MEAN=/HW1/mean
HDFS_OUTPUT_DIR_VAR=/HW1/var
HDFS_INPUT_DIR=/AB_NYC_2019.csv 

hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR_MEAN


yarn jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.name="HW1_mean" \
        -files mapper_mean.py,reducer_mean.py \
        -mapper 'python3 mapper_mean.py' \
        -combiner 'python3 reducer_mean.py' \
        -reducer 'python3 reducer_mean.py' \
        -numReduceTasks 1 \
        -input $HDFS_INPUT_DIR \
        -output $HDFS_OUTPUT_DIR_MEAN

hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR_VAR

yarn jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.name="HW1_var" \
        -files mapper_var.py,reducer_var.py \
        -mapper 'python3 mapper_var.py' \
        -combiner 'python3 reducer_var.py' \
        -reducer 'python3 reducer_var.py' \
        -numReduceTasks 1 \
        -input $HDFS_INPUT_DIR \
        -output $HDFS_OUTPUT_DIR_VAR

hdfs dfs -cat $HDFS_OUTPUT_DIR_MEAN/part-00000 | tee hadoop_result_mean.txt
hdfs dfs -cat $HDFS_OUTPUT_DIR_VAR/part-00000 | tee hadoop_result_var.txt
