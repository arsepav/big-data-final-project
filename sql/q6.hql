USE team27_projectdb;

DROP TABLE IF EXISTS q6_results;

SET hive.vectorized.execution.enabled=false;

CREATE TABLE IF NOT EXISTS q6_results AS
WITH ParsedAndRatedHelpfulness AS (
    SELECT
        CAST(SPLIT(helpfulness, '/')[0] AS INT) AS helpful_votes,
        CAST(SPLIT(helpfulness, '/')[1] AS INT) AS total_votes
    FROM reviews_processed
    WHERE
        helpfulness IS NOT NULL
        AND helpfulness RLIKE '^[0-9]+/[0-9]+$'
        AND SIZE(SPLIT(helpfulness, '/')) = 2
),
HelpfulnessPercentage AS (
    SELECT
        helpful_votes,
        total_votes,
        ROUND((helpful_votes / total_votes) * 100.0, 0) AS percentage_helpful
    FROM ParsedAndRatedHelpfulness
    WHERE
        total_votes > 0
)
SELECT
    CASE
        WHEN percentage_helpful >= 0 AND percentage_helpful < 10 THEN '0-9%'
        WHEN percentage_helpful >= 10 AND percentage_helpful < 20 THEN '10-19%'
        WHEN percentage_helpful >= 20 AND percentage_helpful < 30 THEN '20-29%'
        WHEN percentage_helpful >= 30 AND percentage_helpful < 40 THEN '30-39%'
        WHEN percentage_helpful >= 40 AND percentage_helpful < 50 THEN '40-49%'
        WHEN percentage_helpful >= 50 AND percentage_helpful < 60 THEN '50-59%'
        WHEN percentage_helpful >= 60 AND percentage_helpful < 70 THEN '60-69%'
        WHEN percentage_helpful >= 70 AND percentage_helpful < 80 THEN '70-79%'
        WHEN percentage_helpful >= 80 AND percentage_helpful < 90 THEN '80-89%'
        WHEN percentage_helpful >= 90 AND percentage_helpful <= 100 THEN '90-100%'
        ELSE 'other'
    END AS helpfulness_bucket,
    COUNT(*) AS number_of_reviews,
    MIN(percentage_helpful) as sort_order
FROM HelpfulnessPercentage
GROUP BY
    CASE
        WHEN percentage_helpful >= 0 AND percentage_helpful < 10 THEN '0-9%'
        WHEN percentage_helpful >= 10 AND percentage_helpful < 20 THEN '10-19%'
        WHEN percentage_helpful >= 20 AND percentage_helpful < 30 THEN '20-29%'
        WHEN percentage_helpful >= 30 AND percentage_helpful < 40 THEN '30-39%'
        WHEN percentage_helpful >= 40 AND percentage_helpful < 50 THEN '40-49%'
        WHEN percentage_helpful >= 50 AND percentage_helpful < 60 THEN '50-59%'
        WHEN percentage_helpful >= 60 AND percentage_helpful < 70 THEN '60-69%'
        WHEN percentage_helpful >= 70 AND percentage_helpful < 80 THEN '70-79%'
        WHEN percentage_helpful >= 80 AND percentage_helpful < 90 THEN '80-89%'
        WHEN percentage_helpful >= 90 AND percentage_helpful <= 100 THEN '90-100%'
        ELSE 'other'
    END
ORDER BY sort_order;

INSERT OVERWRITE DIRECTORY 'project/output/q6'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
SELECT * FROM q6_results;