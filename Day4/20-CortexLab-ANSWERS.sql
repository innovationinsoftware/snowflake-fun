/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) AI_SENTIMENT — sentiment analysis on unstructured text
2) AI_CLASSIFY — zero-shot text classification with custom categories
3) AI_COMPLETE — generative summarisation with LLM models
4) Chaining AI functions in a single SELECT statement
5) Aggregate analysis using AI functions with GROUP BY
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Sentiment Extraction
-- ──────────────────────────────────────────────────────────────────────────────

USE DATABASE demo_db;
USE SCHEMA public;

SELECT
    id,
    review,
    AI_SENTIMENT(review):categories[0].sentiment::STRING AS sentiment_label,
    AI_SENTIMENT(review):categories[0].score::FLOAT      AS confidence_score
FROM product_reviews
ORDER BY confidence_score DESC;

-- [TEACHING NOTE]
-- AI_SENTIMENT returns a VARIANT with a categories array. Index [0] is the
-- highest-confidence sentiment. The .score value is between 0 and 1 — higher
-- means the model is more confident in that classification.
-- Reviews 2 and 5 (clearly positive) should have high confidence scores.
-- Review 1 ("works great but shipping was delayed") is likely 'mixed' with
-- a moderate confidence score — making it a good discussion example.
-- Common mistake: using :categories[0].label instead of :categories[0].sentiment
-- — the key name is 'sentiment', not 'label'. Students often guess the path
-- incorrectly; encourage them to inspect the raw VARIANT first:
--   SELECT AI_SENTIMENT(review) FROM product_reviews LIMIT 1;
-- Early finishers: count how many reviews per sentiment_label using GROUP BY.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Custom Classification
-- ──────────────────────────────────────────────────────────────────────────────

SELECT
    id,
    review,
    AI_CLASSIFY(
        review,
        ['Positive Experience', 'Negative Experience', 'Mixed Experience']
    ):labels[0]::STRING AS top_category
FROM product_reviews;

-- Answer: The business-domain labels in Demo 3 (Shipping, Product Quality, etc.)
-- produced functional categories — useful for routing tickets or tagging CRM records.
-- The experience-level labels here produce a higher-level sentiment grouping —
-- similar to AI_SENTIMENT but using the user's own label vocabulary.
-- The key difference is that AI_CLASSIFY labels are entirely user-defined,
-- while AI_SENTIMENT uses a fixed Snowflake taxonomy (positive/negative/mixed/neutral).

-- [TEACHING NOTE]
-- Zero-shot classification means the model was never trained on these specific
-- labels — it uses its general language understanding to assign each text to the
-- closest category. This is extremely powerful for rapid prototyping of
-- classification pipelines without labelled training data.
-- Discussion point: "What would happen if you provided contradictory labels like
-- ['Good', 'Not Good', 'OK']?"
-- (Answer: the model would still assign a label, but accuracy would degrade
--  because the labels overlap in meaning. Clear, mutually exclusive labels
--  produce better results.)
-- Common mistake: providing duplicate or semantically overlapping labels —
-- the model may return inconsistent results across rows if labels are ambiguous.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Prompt Engineering
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: Extract complaint
SELECT
    id,
    review,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'What is the main complaint in this review? Answer in one sentence: ' || review
    ) AS complaint
FROM product_reviews;

-- Task B: Translate to Spanish
SELECT
    id,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'Translate this customer review to Spanish: ' || review
    ) AS translated_review
FROM product_reviews;

-- [TEACHING NOTE]
-- Task A: For positive reviews (id=2, id=5) the model should return something
-- like "There is no complaint in this review" or rephrase the lack of complaint —
-- demonstrating that the model understands context, not just keywords.
-- Task B: Translation quality is generally high for Spanish with Llama 3.3 70B.
-- The model preserves proper nouns and product names correctly.
-- Discussion point: "What are the risks of using AI_COMPLETE for production
-- data pipelines?"
-- (Answer: LLM responses are non-deterministic — the same prompt on the same
--  data may return slightly different text on each run. For deterministic
--  pipelines use AI_SENTIMENT or AI_CLASSIFY which return structured VARIANT
--  output with consistent schema. AI_COMPLETE is better suited for human-facing
--  content generation.)
-- Common mistake: concatenating the prompt with || review without a space or
-- colon separator — the model may interpret the boundary between instruction
-- and content incorrectly, degrading output quality.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Full AI Pipeline in One Query
-- ──────────────────────────────────────────────────────────────────────────────

SELECT
    id,
    AI_SENTIMENT(review):categories[0].sentiment::STRING AS sentiment,
    AI_CLASSIFY(
        review,
        ['Shipping', 'Product Quality', 'Customer Support', 'Pricing', 'Packaging']
    ):labels[0]::STRING AS category,
    AI_COMPLETE(
        'snowflake-llama-3.3-70b',
        'In 6 words, what action should the company take: ' || review
    )::STRING AS action_needed
FROM product_reviews
ORDER BY id ASC;

-- [TEACHING NOTE]
-- This query calls three separate LLM inference functions in a single SELECT —
-- each function is evaluated independently per row. Snowflake batches the
-- LLM calls efficiently under the hood, but three functions × five rows = 15
-- LLM inference calls total.
-- The ::STRING cast on AI_COMPLETE is important — it returns a VARIANT by default,
-- and casting to STRING produces a clean text output column.
-- Discussion point: "How would this query change if the reviews were in a table
-- with 10,000 rows instead of 5?"
-- (Answer: the query syntax is identical — Snowflake parallelises the LLM calls
--  across micro-partitions. Response time scales with row count and warehouse size,
--  not query complexity.)
-- Common mistake: omitting ORDER BY id and assuming row order is stable —
-- AI function results are row-independent but result set ordering requires explicit ORDER BY.
