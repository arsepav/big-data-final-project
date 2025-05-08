#!/bin/bash

password=$(head -n 1 ~/secrets/password.psql.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/db.hql

echo "Tables are created"

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/processing_query.hql

echo "Tables are bucketed and partitioned"

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q1.hql
hdfs dfs -cat project/output/q1/* > output/q1.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q2.hql
hdfs dfs -cat project/output/q2/* > output/q2.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q3.hql
hdfs dfs -cat project/output/q3/* > output/q3.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q4.hql
hdfs dfs -cat project/output/q4/* > output/q4.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q5.hql
hdfs dfs -cat project/output/q5/* > output/q5.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p $password -f sql/q6.hql
hdfs dfs -cat project/output/q6/* > output/q6.csv

echo "All the operations are complete"