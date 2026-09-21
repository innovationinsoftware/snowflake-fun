/*----------------Snowflake Fundamentals — Cortex Instructor Demo:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) AI_SENTIMENT — sentiment analysis on unstructured text
2) AI_CLASSIFY — zero-shot text classification with custom categories
3) AI_COMPLETE — generative summarisation with LLM models
4) Chaining AI functions in a single SELECT statement
5) Aggregate analysis using AI functions with GROUP BY
----------------------------------------------------------------------------------*/

/*
================================================================================
  INSTRUCTOR DEMONSTRATION ONLY — NO STUDENT EXECUTION
  For this class, Cortex AI Functions are unavailable in the students' freely
  created trial accounts. Students observe and discuss the results.
  Demonstrate from an authorized Snowflake account with Cortex access, or show
  screenshots and saved results from actual execution in such an account.

  Suggested delivery: Demos 2–4 are the core presentation (10–15 minutes).
  Demos 5–6 are optional if time permits. Demo 1 is instructor preparation.

  INSTRUCTOR PREPARATION
  - Use the five fictional reviews below; do not use corporate/customer data.
  - Confirm Cortex privileges, warehouse access, and model availability in the
    demonstration account. Existing SQL uses snowflake-llama-3.3-70b: validate
    this model in that account and replace all occurrences if needed.
  - The setup below assumes a dedicated class environment with DEMO_DB.
    It changes grants and replaces DEMO_DB.PUBLIC.PRODUCT_REVIEWS. For a work
    account, have its administrator approve/adapt the role and target schema
    before running setup. Do not run these grants against shared production data.
  - Execute one demo at a time. Capture SQL, input text, and actual output for
    Demos 2, 3, and both prompts in Demo 4. Keep optional captures for Demos 5–6.
  - Save screenshots as 20-Cortex-02-Sentiment.png, 20-Cortex-03-Classify.png,
    20-Cortex-04a-Summary.png, and 20-Cortex-04b-ShortSummary.png.
    These are planned filenames; screenshots are not supplied with this script.
  - Also export/copy the actual results for readable offline display. Record
    the execution date and model used. Never invent successful output.
  - Hide account details and unrelated objects before capturing screenshots.
  - Review model output before teaching; wording and labels can vary by run.

  SCREENSHOT DELIVERY
  Explain each query, show its captured output, and discuss the observations
  listed at the end. Identify captures as previously executed results.
  No student account setup, query execution, or submitted answers are required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup and Sample Data
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- These queries invoke Cortex AI functions through Snowflake SQL.
-- Account, region, model availability, and access policies determine execution.
-- This demonstration uses scalar AI function calls in SELECT statements.
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
-- AI_SENTIMENT returns an object with a categories array. Each entry has
-- a name and sentiment label; the overall category is always present.
-- The documented result does not include a numeric confidence score.
-- The function classifies each row independently — no model training or setup required.
-- With no aspect list supplied here, inspect the overall sentiment result.

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
-- business-relevant labels. The result has a labels array. In the default
-- single-label mode used here, that array contains one selected category.
-- Do not describe the array as a confidence ranking.

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
-- All three functions appear in the same SELECT as separate expressions.
-- These calls do not pass their outputs into one another.
-- The casts extract the overall sentiment and selected classification label.

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
-- OPTIONAL CLEANUP
-- Save screenshots/results before cleanup. Do not drop DEMO_DB; other labs use it.
-- Run only if this demonstration created the table and it is no longer needed:
-- DROP TABLE IF EXISTS demo_db.public.product_reviews;

/*
DISCUSSION — NO SQL EXECUTION REQUIRED
1. Why can a review praising the product still have mixed overall sentiment?
2. Which single category best describes a review mentioning several topics?
3. How does the five-word summary compare with the one-sentence summary?
4. Which AI-generated result would you want a person to review before acting?

There is no student exercise or answer-submission section for this demonstration.
No answer script is needed for this demonstration.

Function reference:
https://docs.snowflake.com/en/sql-reference/functions/ai_sentiment
https://docs.snowflake.com/en/sql-reference/functions/ai_classify
https://docs.snowflake.com/en/sql-reference/functions/ai_complete
*/
