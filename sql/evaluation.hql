USE team27_projectdb;

DROP TABLE IF EXISTS evaluation_results;

CREATE EXTERNAL TABLE evaluation_results (
    model_name STRING,
    rmse DOUBLE,
    mae DOUBLE,
    precision_at_5 DOUBLE,
    ndcg DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team27/project/output/evaluation'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM evaluation_results LIMIT 5;
