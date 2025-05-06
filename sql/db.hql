DROP DATABASE IF EXISTS team27_projectdb CASCADE;

CREATE DATABASE team27_projectdb 
LOCATION 'project/hive/warehouse';

USE team27_projectdb;

CREATE EXTERNAL TABLE books
STORED AS AVRO
LOCATION 'project/warehouse/books'
TBLPROPERTIES (
  'avro.schema.url' = 'project/warehouse/avsc/books.avsc'
);

CREATE EXTERNAL TABLE reviews
STORED AS AVRO
LOCATION 'project/warehouse/reviews'
TBLPROPERTIES (
  'avro.schema.url' = 'project/warehouse/avsc/reviews.avsc'
);

SHOW DATABASES;
USE team27_projectdb;
SHOW TABLES;
DESCRIBE FORMATTED books;
DESCRIBE FORMATTED reviews;