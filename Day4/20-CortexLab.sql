/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) AI_SENTIMENT — sentiment analysis on unstructured text
2) AI_CLASSIFY — zero-shot text classification with custom categories
3) AI_COMPLETE — generative summarisation with LLM models
4) Chaining AI functions in a single SELECT statement
5) Aggregate analysis using AI functions with GROUP BY
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup and Sample Data
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Cortex AI functions run inside Snowflake — the data never leaves the platform.
-- All three functions (AI_SENTIMENT, AI_CLASSIFY, AI_COMPLETE) are scalar SQL
-- functions callable in any SELECT, WHERE, or GROUP BY clause.
-- The product_reviews table contains five realistic reviews that deliberately
-- span multiple sentiment categories to demonstrate meaningful classification.

USE ROLE accountadmin;
GRANT ALL ON DATABASE demo_db TO ROLE sysadmin;
GRANT ALL ON SCHEMA demo_db.public TO ROLE sysadmin;

USE ROLE sysadmin;
USE DATABASE demo_db;
USE SCHEMA public;

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


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ AI_SENTIMENT — Sentiment Analysis
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- AI_SENTIMENT returns a VARIANT with a categories array. Each element contains
-- a sentiment label (positive / negative / mixed / neutral) and a confidence score.
-- The function classifies each row independently — no model training or setup required.
-- Snowflake runs the LLM inference inside the Snowflake data cloud boundary,
-- satisfying data residency requirements out of the box.

SELECT
    id,
    review,
    AI_SENTIMENT(review) AS sentiment
FROM product_reviews;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ AI_CLASSIFY — Zero-Shot Text Classification
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- AI_CLASSIFY assigns each input string to one of the provided category labels
-- without any prior training examples (zero-shot classification).
-- The category list is defined inline as an array — it can be any set of
-- business-relevant labels. The function returns a VARIANT with a labels array
-- ordered by confidence score descending.

SELECT
    id,
    review,
    AI_CLASSIFY(
        review,
        ['Shipping', 'Product Quality', 'Customer Support', 'Pricing', 'Packaging']
    ) AS category
FROM product_reviews;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ AI_COMPLETE — Generative Summarisation
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- AI_COMPLETE sends a prompt to a hosted LLM (snowflake-llama-3.3-70b) and
-- returns the generated text response as a STRING.
-- The prompt is constructed by concatenating a static instruction with the
-- review text — the model responds to the combined prompt for each row.
-- Response length and style vary with prompt wording; shorter prompts
-- ("in 3 words") produce more constrained output.

SELECT
    id,
    review,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'Summarize in one short sentence: ' || review
    ) AS summary
FROM product_reviews;

SELECT
    id,
    review,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'Summarize in 5 words: ' || review
    ) AS ultra_short_summary
FROM product_reviews;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Chaining AI Functions in One Query
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- All three AI functions can appear in the same SELECT — each evaluates
-- independently per row. This replaces a pipeline that would previously require
-- three separate API calls, a Python service, and data movement between systems.
-- The ::string casts extract the top label from each VARIANT result for clean output.

SELECT
    id,
    AI_SENTIMENT(review):categories[0].sentiment::STRING AS sentiment,
    AI_CLASSIFY(
        review,
        ['Shipping', 'Product Quality', 'Customer Support', 'Pricing', 'Packaging']
    ):labels[0]::STRING                                  AS category,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'Summarize in 4 words: ' || review
    )::STRING                                            AS short_summary
FROM product_reviews;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ Aggregate Analysis with AI Functions
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- AI functions work inside aggregate queries exactly like built-in scalar
-- functions. GROUP BY on the extracted sentiment label produces a summary
-- count per sentiment category — a common dashboard metric derived entirely
-- from unstructured text with no pre-processing.

SELECT
    AI_SENTIMENT(review):categories[0].sentiment::STRING AS sentiment,
    COUNT(*) AS total
FROM product_reviews
GROUP BY 1
ORDER BY total DESC;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- demo_db must NOT be dropped — used throughout Day 4.
-- product_reviews is kept for the student exercises.


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  All exercises are READ-ONLY — no CREATE, INSERT, UPDATE, or DROP required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Sentiment Extraction
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a query against product_reviews that extracts the top sentiment
--       label and its confidence score from AI_SENTIMENT.
--       Return: id, review, sentiment_label, confidence_score
--       where:
--         sentiment_label   = AI_SENTIMENT(review):categories[0].sentiment::STRING
--         confidence_score  = AI_SENTIMENT(review):categories[0].score::FLOAT
--       Order by confidence_score DESC.

USE DATABASE demo_db;
USE SCHEMA public;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Custom Classification
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Use AI_CLASSIFY to categorise each review into one of these labels:
--         ['Positive Experience', 'Negative Experience', 'Mixed Experience']
--       Return: id, review, top_category
--       where top_category = AI_CLASSIFY(...):labels[0]::STRING
--       How does the result differ from Demo 3 which used business-domain labels?
--       Answer in a comment below your query.

-- YOUR CODE HERE

-- Answer:


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Prompt Engineering
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write two AI_COMPLETE queries against product_reviews:
--         A) Prompt: 'What is the main complaint in this review? Answer in one
--            sentence: ' || review
--            Return id, review, complaint
--         B) Prompt: 'Translate this customer review to Spanish: ' || review
--            Return id, translated_review
--       For reviews with no complaint (positive), what does the model return?

-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Full AI Pipeline in One Query
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a single SELECT that produces an executive summary table
--       with these columns for every row in product_reviews:
--         id
--         sentiment      — top sentiment label (::STRING)
--         category       — top business category from this list:
--                          ['Shipping','Product Quality','Customer Support',
--                           'Pricing','Packaging']
--         action_needed  — AI_COMPLETE result for the prompt:
--                          'In 6 words, what action should the company take: ' || review
--       Order by id ASC.

-- YOUR CODE HERE
