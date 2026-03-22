/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) ROW_NUMBER, RANK, DENSE_RANK analytic functions
2) QUALIFY clause — inline filtering on analytic function results
3) MIN/MAX as analytic functions with PARTITION BY
4) Multiple analytic functions in a single query
5) Advanced QUALIFY challenges — presidents, top-paid clerks, department averages
6) Row-value subquery comparisons
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ QUALIFY with ROW_NUMBER
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Highest-paid employee per department using ROW_NUMBER + QUALIFY.

USE SCHEMA demo_db.scott;

SELECT
    empno,
    ename,
    job,
    sal,
    deptno
FROM emp
QUALIFY ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC) = 1;

-- [TEACHING NOTE]
-- ROW_NUMBER assigns 1 to the highest-sal row per deptno (ORDER BY sal DESC).
-- QUALIFY filters before the outer ORDER BY, eliminating the CTE wrapper.
-- If two employees have the same salary within a department, ROW_NUMBER
-- breaks the tie arbitrarily — use ORDER BY sal DESC, empno ASC for
-- deterministic results in production.
-- Discussion point: how would RANK() differ from ROW_NUMBER() here?
-- (Answer: RANK() would return multiple rows for tied salaries, whereas
--  ROW_NUMBER() always returns exactly one per partition.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Analytic Aggregates
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Add dept_avg_sal, dept_max_sal, and pct_of_max to all employee rows.

SELECT
    empno,
    ename,
    job,
    sal,
    deptno,
    ROUND(AVG(sal) OVER(PARTITION BY deptno), 2)              AS dept_avg_sal,
    MAX(sal) OVER(PARTITION BY deptno)                         AS dept_max_sal,
    ROUND(sal * 100.0 / MAX(sal) OVER(PARTITION BY deptno), 1) AS pct_of_max
FROM emp
ORDER BY deptno, sal DESC;

-- [TEACHING NOTE]
-- All three analytic functions use PARTITION BY deptno — each is evaluated
-- independently per department. They can all coexist in the same SELECT.
-- pct_of_max requires casting to avoid integer division: sal * 100.0 (not 100).
-- Common mistake: using AVG(sal) / MAX(sal) in the same expression without
-- OVER() — this would be a grouped aggregate, not an analytic function,
-- and would require GROUP BY or a subquery.
-- Early finishers: add RANK() OVER(PARTITION BY deptno ORDER BY sal DESC) AS
-- sal_rank to show each employee's salary rank within their department.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ QUALIFY with Conditional Aggregates
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Return all employees in the same department as KING, using QUALIFY.

SELECT *
FROM emp
QUALIFY COUNT(CASE ename WHEN 'KING' THEN 1 END) OVER(PARTITION BY deptno) > 0
ORDER BY deptno;

-- [TEACHING NOTE]
-- The CASE WHEN produces 1 for KING's row and NULL for all others.
-- COUNT ignores NULLs, so it returns 1 for KING's department and 0 for others.
-- QUALIFY filters to departments where the count > 0 — all employees in that dept.
-- This single-scan pattern is equivalent to:
--   WHERE deptno IN (SELECT deptno FROM emp WHERE ename = 'KING')
-- but avoids a correlated subquery and scans emp only once.
-- Common mistake: using SUM(CASE WHEN ename = 'KING' THEN 1 ELSE 0 END) —
-- this works but uses ELSE 0, which means COUNT and SUM behave differently
-- (COUNT skips NULLs; SUM adds 0s). Both are correct; NULL-based COUNT is
-- more conventional in this pattern.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Duplicate Detection with ROW_NUMBER
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Return exactly one employee per (deptno, job) — lowest empno wins.

SELECT
    empno,
    ename,
    job,
    deptno,
    sal
FROM emp
QUALIFY ROW_NUMBER() OVER(PARTITION BY deptno, job ORDER BY empno ASC) = 1
ORDER BY deptno, job;

-- [TEACHING NOTE]
-- PARTITION BY deptno, job groups rows with the same department AND job together.
-- ORDER BY empno ASC means the row with the lowest empno gets rn = 1.
-- This is the standard deduplication query used in production ETL pipelines:
-- "for each natural key combination, keep only the earliest record."
-- Discussion point: what would change if you used RANK() instead of ROW_NUMBER()?
-- (Answer: if two employees share the lowest empno in a (deptno, job) partition —
--  which is impossible here since empno is a primary key — RANK() would return
--  both. For true deduplication, ROW_NUMBER() is the correct choice.)
-- Early finishers: extend the query to also return the count of duplicates
-- per (deptno, job) using COUNT(*) OVER(PARTITION BY deptno, job) AS dup_count.
