USE team27_projectdb;

DROP TABLE IF EXISTS reviews_processed;

CREATE EXTERNAL TABLE reviews_processed (
    book_id STRING,
    book_title STRING,
    price STRING,
    user_id STRING,
    profile_name STRING,
    helpfulness STRING,
    review_time BIGINT,
    summary STRING,
    review_text STRING
)
PARTITIONED BY (score STRING)
CLUSTERED BY (book_id) INTO 10 BUCKETS
STORED AS AVRO
LOCATION 'project/warehouse/reviews_processed'
TBLPROPERTIES (
    'avro.schema.url' = 'project/warehouse/avsc/reviews_partitioned.avsc'
);

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 2000;

INSERT OVERWRITE TABLE reviews_processed PARTITION (score)
SELECT 
    book_id, 
    book_title, 
    price, 
    user_id, 
    profile_name, 
    helpfulness, 
    review_time, 
    summary, 
    review_text, 
    score
FROM reviews;

SHOW PARTITIONS reviews_processed;
DESCRIBE FORMATTED reviews_processed;

DROP TABLE IF EXISTS reviews;


