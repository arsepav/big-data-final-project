USE team27_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE TABLE IF NOT EXISTS q3_results AS
SELECT
    CAST(CAST(YEAR(FROM_UNIXTIME(review_time)) AS INT) / 365.25 + 1900 AS INT) AS review_year,
    COUNT(*) AS number_of_reviews
FROM
    reviews_processed
WHERE
    review_time IS NOT NULL
    AND review_time > 0
GROUP BY
    CAST(CAST(YEAR(FROM_UNIXTIME(review_time)) AS INT) / 365.25 + 1900 AS INT)
ORDER BY
    review_year;


INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
SELECT * FROM q3_results;
