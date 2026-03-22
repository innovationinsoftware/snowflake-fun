/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) ROW_NUMBER, RANK, DENSE_RANK analytic functions
2) QUALIFY clause — inline filtering on analytic function results
3) MIN/MAX as analytic functions with PARTITION BY
4) Multiple analytic functions in a single query
5) Advanced QUALIFY challenges — presidents, top-paid clerks, department averages
6) Row-value subquery comparisons
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context and Approaches to "Top N per Group"
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Four progressively cleaner approaches retrieve the single earliest-hired
-- employee. The sequence shows that QUALIFY eliminates the need for a CTE
-- wrapper and is unique to Snowflake (and a few other modern SQL engines).
-- Discussion point: "Which approach is most readable for a new team member?"
-- (Answer: subjective, but QUALIFY inline is the most concise.)

USE SCHEMA demo_db.scott;

-- 1a. TOP option (Snowflake extension)
SELECT TOP 1 *
FROM emp
ORDER BY hiredate;

-- 1b. LIMIT clause (ANSI standard)
SELECT *
FROM emp
ORDER BY hiredate
LIMIT 1;

-- 1c. CTE with ROW_NUMBER
WITH x AS (
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY hiredate) AS rn
    FROM emp
)
SELECT *
FROM x
WHERE rn = 1;

-- 1d. ROW_NUMBER with QUALIFY
SELECT
    *,
    ROW_NUMBER() OVER(ORDER BY hiredate) AS rn
FROM emp
QUALIFY rn = 1;

-- 1e. Inline QUALIFY — most concise form
SELECT *
FROM emp
QUALIFY ROW_NUMBER() OVER(ORDER BY hiredate) = 1;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ First Hire per Department — CTE vs Analytic
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Two strategies for a "first per group" problem:
--   Strategy 1 uses a GROUP BY CTE + JOIN — two passes over the table.
--   Strategy 3 uses MIN() as an analytic function + QUALIFY — single pass.
-- The analytic approach is typically more efficient on large datasets.

-- 2a. CTE strategy: find MIN hiredate per department, then join back
SELECT
    deptno,
    MIN(hiredate) AS first_hiredate
FROM emp
GROUP BY deptno
ORDER BY 1;

WITH x AS (
    SELECT
        deptno,
        MIN(hiredate) AS first_hiredate
    FROM emp
    GROUP BY deptno
)
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND e.hiredate = x.first_hiredate
ORDER BY e.deptno;

-- 2b. Analytic strategy: MIN() OVER(PARTITION BY) + QUALIFY — single scan
SELECT *
FROM emp
QUALIFY hiredate IN (
    MIN(hiredate) OVER(PARTITION BY deptno),
    MAX(hiredate) OVER(PARTITION BY deptno)
)
ORDER BY deptno, hiredate;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Multiple Analytic Functions in One Query
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A single SELECT can contain multiple OVER() clauses with different
-- PARTITION BY and ORDER BY expressions simultaneously.
-- Each analytic function is evaluated independently against the full result set
-- before any QUALIFY or ORDER BY is applied — there is no inter-dependency.

SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno,
    sal,
    MIN(sal)    OVER(PARTITION BY job)               AS min_sal_job,
    ROW_NUMBER() OVER(PARTITION BY job ORDER BY sal) AS rn_job_sal,
    RANK()      OVER(ORDER BY sal)                   AS rk_sal,
    DENSE_RANK() OVER(ORDER BY sal)                  AS drk_sal,
    COUNT(*)    OVER(PARTITION BY deptno)            AS dept_count,
    COUNT(*)    OVER()                               AS total_count
FROM emp
ORDER BY sal;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Advanced QUALIFY Challenges
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The four challenges show QUALIFY handling progressively harder business
-- problems that would require correlated subqueries in traditional SQL.
-- Each builds on the previous — position QUALIFY as "WHERE for window functions".

-- 4a. Challenge: employees in the same department as the president
SELECT *
FROM emp
QUALIFY COUNT(CASE WHEN job = 'PRESIDENT' THEN 1 END) OVER(PARTITION BY deptno) > 0
ORDER BY deptno;

-- 4b. Challenge: employees in the department of the top-paid clerk
SELECT *
FROM emp a
QUALIFY MAX(CASE WHEN job = 'CLERK' THEN sal END) OVER(PARTITION BY deptno) =
        MAX(CASE WHEN job = 'CLERK' THEN sal END) OVER()
ORDER BY deptno;

-- 4c. Challenge: employees paid above their department average
SELECT *
FROM emp e
QUALIFY sal > AVG(sal) OVER(PARTITION BY deptno)
ORDER BY deptno;

-- 4d. Challenge: employees with the same department and job as ADAMS
SELECT *
FROM emp
QUALIFY COUNT(CASE ename WHEN 'ADAMS' THEN 1 END) OVER(PARTITION BY deptno, job) > 0
ORDER BY deptno, job;


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  All exercises are READ-ONLY — no CREATE, INSERT, UPDATE, or DROP required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ QUALIFY with ROW_NUMBER
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using the emp table in demo_db.scott, write a single SELECT (no CTE)
--       that returns the highest-paid employee in each department.
--       Use ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC) and
--       filter with QUALIFY.
--       Return: empno, ename, job, sal, deptno.

USE SCHEMA demo_db.scott;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Analytic Aggregates
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a query that returns all employees with these additional columns:
--         dept_avg_sal   — average salary for the employee's department
--         dept_max_sal   — maximum salary for the employee's department
--         pct_of_max     — employee's salary as a percentage of the dept max,
--                          rounded to 1 decimal place
--       Order by deptno, sal DESC.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ QUALIFY with Conditional Aggregates
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a single query (no subquery, no CTE) that returns all employees
--       who work in the same department as the employee named 'KING'.
--       Use QUALIFY with a COUNT(CASE ... END) OVER(PARTITION BY deptno) pattern.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Duplicate Detection with ROW_NUMBER
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: The table contains employees with the same job in the same department.
--       Write a query that returns only ONE employee per (deptno, job) combination
--       — keep the one with the lowest empno (use ORDER BY empno ASC).
--       Use ROW_NUMBER() OVER(PARTITION BY deptno, job ORDER BY empno) with
--       QUALIFY to filter to rn = 1.
--       Return: empno, ename, job, deptno, sal.
--       Hint: this is the deduplication pattern used in production pipelines.


-- YOUR CODE HERE
