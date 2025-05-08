#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf

#spark-submit --master yarn preprocessing.py

hdfs dfs -getmerge project/data/train data/train.json

hdfs dfs -getmerge project/data/test data/test.json

# hdfs dfs -mkdir -p project/output/evaluation

# hdfs dfs -put -f output/evaluation.csv project/output/evaluation

