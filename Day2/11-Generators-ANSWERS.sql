/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) TABLE(GENERATOR) — row count-based sequence generation
2) CONNECT BY as a range generator
3) Recursive CTEs as range generators
4) Integer sequences, random values, random strings
5) Date range generation — fixed count, end-of-month, end-of-week
6) Month name and alphabet generation
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Sequence Generation
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Generate odd integers 1, 3, 5 … 19 using TABLE(GENERATOR).

SELECT (ROW_NUMBER() OVER(ORDER BY 1) * 2) - 1 AS value
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- [TEACHING NOTE]
-- ROW_NUMBER() produces 1..10; multiplying by 2 and subtracting 1 maps to
-- 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 — exactly 10 odd numbers.
-- The same formula works for even numbers: ROW_NUMBER() * 2 gives 2..20.
-- Discussion point: how would you generate every 3rd integer (3, 6, 9 …)?
-- (Answer: ROW_NUMBER() * 3.)
-- Common mistake: using ROWCOUNT => 19 and trying to filter WHERE MOD(value,2)=1
-- — that works but wastes rows. Computing the value directly is more efficient.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Date Sequence with Day Name
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: All dates in next calendar month using a recursive CTE.

WITH RECURSIVE x(date_value) AS (
    SELECT DATE_TRUNC('month', DATEADD(month, 1, CURRENT_DATE))::DATE
    UNION ALL
    SELECT DATEADD(DAY, 1, x.date_value)::DATE
    FROM x
    WHERE MONTH(DATEADD(DAY, 1, x.date_value)) =
          MONTH(DATE_TRUNC('month', DATEADD(month, 1, CURRENT_DATE)))
)
SELECT
    date_value,
    TO_CHAR(date_value, 'Dy') AS day_name
FROM x;

-- [TEACHING NOTE]
-- The anchor is the first day of next month: DATE_TRUNC('month', DATEADD(month,1,...)).
-- The recursive step adds one day and terminates when the month of the new date
-- differs from the target month — naturally stopping at month-end.
-- Casting to ::DATE is important: DATEADD returns TIMESTAMP by default in some
-- contexts; the explicit cast keeps the column type clean.
-- Common mistake: using CURRENT_DATE + 1 as the anchor — this starts from
-- tomorrow, not the first of next month.
-- Early finishers: modify the CTE to also include a column for the ISO week
-- number: TO_CHAR(date_value, 'IW') AS iso_week.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Random Data Set
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: 20-row synthetic employee table with emp_id, emp_name, salary, dept_id.

SELECT
    ROW_NUMBER() OVER(ORDER BY 1)          AS emp_id,
    UPPER(RANDSTR(6, RANDOM()))            AS emp_name,
    UNIFORM(30000, 120000, RANDOM())::INT  AS salary,
    UNIFORM(1, 5, RANDOM())::INT           AS dept_id
FROM TABLE(GENERATOR(ROWCOUNT => 20))
ORDER BY emp_id;

-- [TEACHING NOTE]
-- Each call to RANDOM() inside a single row produces a different seed —
-- so emp_name, salary, and dept_id are independently randomised per row.
-- The ::INT cast on UNIFORM ensures the output is displayed as an integer
-- rather than a FLOAT (UNIFORM returns FLOAT by default).
-- Discussion point: why does re-running this query produce different results?
-- (Answer: RANDOM() is non-deterministic — each execution gets a fresh seed.
--  For reproducible results, pass a fixed integer seed: RANDOM(42).)
-- Common mistake: using RANDSTR(6, RANDOM()) without UPPER() and expecting
-- uppercase output — RANDSTR returns mixed-case alphanumeric by default.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Multiplication Table
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: 5×5 multiplication table using two GENERATOR-based CTEs cross-joined.

WITH r AS (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) AS row_n
    FROM TABLE(GENERATOR(ROWCOUNT => 5))
),
c AS (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) AS col_n
    FROM TABLE(GENERATOR(ROWCOUNT => 5))
)
SELECT
    r.row_n,
    c.col_n,
    r.row_n * c.col_n AS product
FROM r
CROSS JOIN c
ORDER BY r.row_n, c.col_n;

-- [TEACHING NOTE]
-- GENERATOR-based CTEs are the cleanest approach here: no recursion depth limit,
-- no CONNECT BY dual trick needed, and the ROWCOUNT is immediately obvious.
-- A CROSS JOIN of two 5-row sets produces 5×5 = 25 rows — every (row_n, col_n)
-- combination.

