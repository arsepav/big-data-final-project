#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf

spark-submit --master yarn preprocessing.py

hdfs dfs -getmerge project/data/train data/train.json

hdfs dfs -getmerge project/data/test data/test.json

spark-submit --master yarn models.py

hdfs dfs -get -project/output/evaluation /home/team27/output/evaluation.csv

