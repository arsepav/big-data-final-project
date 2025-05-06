USE team27_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE TABLE IF NOT EXISTS q5_results AS
SELECT
    user_id,
    MAX(profile_name) as profile_name,
    COUNT(*) AS number_of_reviews
FROM
    reviews_processed
WHERE
    user_id IS NOT NULL
GROUP BY
    user_id
ORDER BY
    number_of_reviews DESC
LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
SELECT * FROM q5_results;
