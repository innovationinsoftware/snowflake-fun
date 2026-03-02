/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) AI_SENTIMENT function
2) AI_CLASSIFY function
3) AI_COMPLETE function
4) Chaining AI functions
5) Advanced prompting
----------------------------------------------------------------------------------*/

-- Step 0 – Setup

USE ROLE sysadmin;
USE DATABASE <your_db>;
USE SCHEMA <your_schema>;

CREATE OR REPLACE TABLE product_reviews (
    id     INT,
    review STRING
);

INSERT INTO product_reviews VALUES
(1, 'The laptop works great but shipping was delayed by two weeks.'),
(2, 'Excellent quality and fast delivery. Very satisfied with the purchase.'),
(3, 'Customer support was unhelpful and the product stopped working after one month.'),
(4, 'Good value for the price, but packaging could be improved.'),
(5, 'Amazing performance and sleek design. Highly recommended.');


-- Step 1 – Sentiment Analysis

SELECT
    id,
    review,
    AI_SENTIMENT(review) AS sentiment
FROM product_reviews;

/*
Positive / Negative / Neutral classification
Instant AI analysis without moving data
AI is running where the data lives.
*/


-- Step 2 – Business Classification

SELECT
    id,
    review,
    AI_CLASSIFY(
        review,
        ['Shipping', 'Product Quality', 'Customer Support', 'Pricing', 'Packaging']
    ) AS category
FROM product_reviews;

-- Structured output from unstructured text
-- Business automation potential


-- Step 3 – Generative Summarization

SELECT
    id,
    AI_COMPLETE('Summarize in one short sentence: ' || review) AS summary
FROM product_reviews;

SELECT
    id,
    AI_COMPLETE('Summarize in 5 words: ' || review) AS ultra_short_summary
FROM product_reviews;


-- Step 4 – Chaining AI Functions

SELECT
    id,
    AI_SENTIMENT(review) AS sentiment,
    AI_CLASSIFY(
        review,
        ['Shipping', 'Product Quality', 'Customer Support', 'Pricing', 'Packaging']
    ) AS category,
    AI_COMPLETE('Summarize in 5 words: ' || review) AS short_summary
FROM product_reviews;

/*
Sentiment analysis
Text classification
Generative summarization
All in one SQL query
No external ML platform
No Python
No API calls
*/


-- Step 5 – Optional Advanced Prompting

SELECT
    id,
    AI_COMPLETE(
        'Extract only the main issue from this review in 3 words: ' || review
    ) AS main_issue
FROM product_reviews;


-- Step 6 – Optional Enhancement: Aggregate sentiment counts

SELECT
    AI_SENTIMENT(review) AS sentiment,
    COUNT(*) AS total
FROM product_reviews
GROUP BY sentiment;