#!/bin/bash

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team27 -p "$(head -n 1 ~/secrets/password.psql.pass)" -f sql/evaluation.hql
