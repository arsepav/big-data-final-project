USE team27_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE TABLE IF NOT EXISTS q4_results AS
WITH PublisherReviews AS (
    SELECT
        b.publisher as publisher,
        r.score as score
    FROM
        reviews_processed r
    JOIN
        books b ON TRIM(LOWER(r.book_title)) = TRIM(LOWER(b.title))
    WHERE
        r.score IS NOT NULL
        AND b.publisher IS NOT NULL AND b.publisher != ''
),
PublisherStats AS (
    SELECT
        publisher,
        COUNT(*) as total_reviews,
        AVG(score) as avg_score
    FROM PublisherReviews
    GROUP BY publisher
)
SELECT
    publisher,
    avg_score,
    total_reviews
FROM PublisherStats
WHERE total_reviews > 20
ORDER BY avg_score DESC, total_reviews DESC
LIMIT 20;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
SELECT * FROM q4_results;
