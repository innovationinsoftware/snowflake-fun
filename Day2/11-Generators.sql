/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) TABLE(GENERATOR) — row count-based sequence generation
2) CONNECT BY as a range generator
3) Recursive CTEs as range generators
4) Integer sequences, random values, random strings
5) Date range generation — fixed count, end-of-month, end-of-week
6) Month name and alphabet generation
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Integer Sequences — Three Generator Strategies
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Three native strategies generate a sequential integer list without any base table:
--   GENERATOR(ROWCOUNT => N) — Snowflake-native, most concise and scalable
--   CONNECT BY with LIMIT    — Oracle-compatible syntax, uses the dual table
--   Recursive CTE            — ANSI SQL standard, explicit anchor + recursive step
-- All three produce the same result; GENERATOR is preferred for large N because
-- it does not recurse and is not limited by query depth settings.

-- 1a. TABLE(GENERATOR)
SELECT ROW_NUMBER() OVER(ORDER BY 1) AS value
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- 1b. CONNECT BY
SELECT LEVEL AS value
FROM dual
CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
LIMIT 10;

-- 1c. Recursive CTE
WITH x(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1
    FROM x
    WHERE x.value < 10
)
SELECT *
FROM x;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Random Integers and Random Strings
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- UNIFORM(low, high, RANDOM()) generates random integers in a closed range.
-- RANDSTR(len, RANDOM()) generates a random alphanumeric string of length len.
-- Both functions take a RANDOM() seed so each row gets a different value.
-- The generator produces the required number of rows; the random function
-- fills each row with a fresh value.

-- 2a. 10 random integers between 20 and 50
SELECT UNIFORM(20::INT, 50::INT, RANDOM()) AS rnd
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- 2b. 10 random 5-character strings
SELECT UPPER(RANDSTR(5, RANDOM())) AS random_str
FROM TABLE(GENERATOR(ROWCOUNT => 10));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Date Range Generation
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- GENERATOR combined with ROW_NUMBER() and DATEADD produces date sequences.
-- The row count for "rest of month" and "rest of week" is computed inline using
-- DAY(LAST_DAY(...)) arithmetic — no hard-coded row counts needed.
-- Recursive CTE variants are shown as ANSI-standard alternatives.

-- 3a. Fixed 10-day sequence from today
SELECT CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 AS "date"
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- 3b. Remaining days of the current month
SELECT CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 AS "date"
FROM TABLE(GENERATOR(
    ROWCOUNT => 1 + DAY(LAST_DAY(CURRENT_DATE)) - DAY(CURRENT_DATE)
));

-- 3c. Remaining days of the current week (with day name)
SELECT
    CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 AS "date",
    TO_CHAR("date", 'Dy')                              AS "day"
FROM TABLE(GENERATOR(
    ROWCOUNT => 1 + DAY(LAST_DAY(CURRENT_DATE, WEEK)) - DAY(CURRENT_DATE)
));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Month Names and Alphabet Generation
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- ADD_MONTHS advances a date by N months; TO_CHAR with 'MMMM' extracts the name.
-- Anchoring at TRUNC(CURRENT_DATE, 'Year') gives January 1 of the current year,
-- then offsetting by ROW_NUMBER()-1 steps through all 12 months.
-- The alphabet uses ASCII arithmetic: CHR(ASCII('A') + n) where n = 0..25.

-- 4a. All 12 month names
SELECT TO_CHAR(
    ADD_MONTHS(TRUNC(CURRENT_DATE, 'Year'), ROW_NUMBER() OVER(ORDER BY 1) - 1),
    'MMMM'
) AS month
FROM TABLE(GENERATOR(ROWCOUNT => 12));

-- 4b. Alphabet A–Z
SELECT CHR(ASCII('A') + ROW_NUMBER() OVER(ORDER BY 1) - 1) AS letter
FROM TABLE(GENERATOR(ROWCOUNT => 26));


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  All exercises are READ-ONLY — no CREATE, INSERT, UPDATE, or DROP required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Sequence Generation
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Generate a list of ODD integers from 1 to 19 (i.e., 1, 3, 5, … 19)
--       using TABLE(GENERATOR).
--       Hint: generate 10 rows and compute value = (ROW_NUMBER() * 2) - 1.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Date Sequence with Day Name
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using a recursive CTE, generate all dates in the NEXT calendar month
--       (not the current month — the month after today).
--       Columns: date_value DATE, day_name VARCHAR (e.g. 'Mon').
--       Hint: anchor at DATE_TRUNC('month', DATEADD(month, 1, CURRENT_DATE))
--             and recurse while MONTH(date_value + 1) = MONTH(anchor).


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Random Data Set
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Generate a synthetic employee table with 20 rows using GENERATOR.
--       Each row must have:
--         emp_id     — sequential integer from 1 to 20
--         emp_name   — random 6-character uppercase string
--         salary     — random integer between 30000 and 120000
--         dept_id    — random integer between 1 and 5
--       Return all 20 rows ordered by emp_id.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Multiplication Table
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Generate a 5×5 multiplication table using two GENERATOR-based CTEs
--       (one for rows, one for columns) cross-joined together.
--       Columns: row_n INT, col_n INT, product INT
--       where product = row_n * col_n.
--       Return all 25 rows ordered by row_n, col_n.
--       Hint: use two CTEs each generating integers 1–5, then CROSS JOIN them.


-- YOUR CODE HERE
