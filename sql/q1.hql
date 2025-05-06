USE team27_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE TABLE IF NOT EXISTS q1_results AS
SELECT
    CAST(score AS INT) AS rating,
    COUNT(*) AS number_of_reviews
FROM
    reviews_processed
WHERE
    score IS NOT NULL
GROUP BY
    CAST(score AS INT)
ORDER BY
    rating;

INSERT OVERWRITE DIRECTORY 'project/output/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
SELECT * FROM q1_results;
