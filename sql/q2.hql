USE team27_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE TABLE IF NOT EXISTS q2_results AS
SELECT
    book_title,
    COUNT(*) AS reviews_cnt
FROM
    reviews_processed
WHERE
    book_title IS NOT NULL
GROUP BY
    book_title
ORDER BY
    reviews_cnt DESC
LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
SELECT * FROM q2_results;
