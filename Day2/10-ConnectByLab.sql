/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Self-join and UNION ALL for hierarchical data
2) CONNECT BY clause and START WITH
3) LEVEL pseudo-column and SYS_CONNECT_BY_PATH
4) CONNECT_BY_ROOT — multiple starting points
5) Recursive CTEs — WITH RECURSIVE
6) CONNECT BY vs Recursive CTE equivalence
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context and Manager/Employee Relationship
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The classic emp table stores a self-referencing mgr column (manager's empno).
-- The top of the hierarchy has mgr IS NULL (the president has no manager).
-- A self-join on emp e JOIN emp m ON e.mgr = m.empno produces each employee
-- alongside their direct manager in a flat result — no hierarchy depth.

USE DATABASE demo_db;
USE SCHEMA scott;

-- 1a. Raw manager/employee columns
SELECT
    empno,
    ename,
    job,
    mgr
FROM emp;

-- 1b. Self-join to show employee + direct manager in one row
SELECT
    e.empno,
    e.ename,
    e.job,
    e.mgr,
    m.ename AS mgr_name,
    m.job   AS mgr_title
FROM emp e
LEFT JOIN emp m ON e.mgr = m.empno;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ CONNECT BY — Basic Hierarchy Traversal
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- CONNECT BY traverses a parent-child relationship row by row.
-- START WITH defines the root(s) — here, rows where mgr IS NULL (the president).
-- CONNECT BY mgr = PRIOR empno means: "the current row's mgr equals the
-- parent row's empno" — walking down the org chart.
-- LEVEL starts at 1 for root nodes and increments with each level of depth.

-- 2a. Basic hierarchy with LEVEL
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno;

-- 2b. Visual indentation + full path using SYS_CONNECT_BY_PATH
SELECT
    empno,
    LPAD('.', (LEVEL - 1) * 5, '.') || ename AS name,
    job,
    mgr,
    LEVEL,
    LTRIM(SYS_CONNECT_BY_PATH(empno, '>'), '>') AS path
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno
ORDER BY path;

-- 2c. Same result via CTE for reusable ordering
WITH x AS (
    SELECT
        empno,
        LPAD('.', (LEVEL - 1) * 5, '.') || ename AS empl_name,
        job,
        mgr,
        LEVEL                                     AS empl_level,
        LTRIM(SYS_CONNECT_BY_PATH(empno, '>'), '>') AS path
    FROM emp
    START WITH mgr IS NULL
    CONNECT BY mgr = PRIOR empno
)
SELECT
    empno,
    empl_name,
    job,
    mgr,
    empl_level
FROM x
ORDER BY path;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Subtree Traversal — Descendants and Ancestors
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- START WITH any specific empno restricts the hierarchy to that subtree.
-- Direction is controlled by the CONNECT BY clause:
--   CONNECT BY mgr = PRIOR empno → downward (descendants)
--   CONNECT BY PRIOR mgr = empno → upward (ancestors toward the root)

-- 3a. All descendants of employee 7788
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7788
CONNECT BY mgr = PRIOR empno;

-- 3b. All ancestors of employee 7788 (walking up the chain)
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7788
CONNECT BY PRIOR mgr = empno;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ CONNECT_BY_ROOT — Multiple Starting Points
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- CONNECT_BY_ROOT(col) returns the value of col at the root of each subtree.
-- When multiple START WITH roots are specified, each subtree is labelled
-- with its own root value — allowing separate subtrees to be distinguished
-- in a single query result.

SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL,
    CONNECT_BY_ROOT(empno) AS root_empno
FROM emp
START WITH empno IN (7369, 7499)
CONNECT BY PRIOR mgr = empno
ORDER BY root_empno, LEVEL;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Recursive CTE — Equivalent to CONNECT BY
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- WITH RECURSIVE is the ANSI SQL standard for hierarchical queries.
-- Structure: anchor member (root rows) UNION ALL recursive member (join to self).
-- The INTERSECT with the CONNECT BY result proves both approaches return
-- identical rows — CONNECT BY is syntactic sugar over recursive traversal.

-- 5a. Standalone recursive CTE
WITH RECURSIVE x(empno, ename, job, mgr, level_) AS (
    SELECT empno, ename, job, mgr, 1
    FROM emp
    WHERE mgr IS NULL
    UNION ALL
    SELECT e.empno, e.ename, e.job, e.mgr, x.level_ + 1
    FROM emp e
    JOIN x ON e.mgr = x.empno
)
SELECT *
FROM x;

-- 5b. INTERSECT proves CONNECT BY and recursive CTE are equivalent
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL AS level_
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno

INTERSECT

(
    WITH RECURSIVE x(empno, ename, job, mgr, level_) AS (
        SELECT empno, ename, job, mgr, 1
        FROM emp
        WHERE mgr IS NULL
        UNION ALL
        SELECT e.empno, e.ename, e.job, e.mgr, x.level_ + 1
        FROM emp e
        JOIN x ON e.mgr = x.empno
    )
    SELECT *
    FROM x
);


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  All exercises are READ-ONLY — no CREATE, INSERT, UPDATE, or DROP required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Basic CONNECT BY
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a CONNECT BY query that starts at the employee named 'JONES'
--       (use START WITH ename = 'JONES') and traverses all descendants
--       (employees who report, directly or indirectly, to JONES).
--       Return: empno, ename, job, LEVEL.
--       How many levels deep does JONES's subtree go?

USE DATABASE demo_db;
USE SCHEMA scott;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Ancestor Path with SYS_CONNECT_BY_PATH
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a query that walks UP the hierarchy from employee 7876 (ADAMS)
--       to the root, showing each ancestor's name and the full ename path.
--       Use: START WITH empno = 7876
--            CONNECT BY PRIOR mgr = empno
--            SYS_CONNECT_BY_PATH(ename, ' -> ') to build the path
--       Return: empno, ename, job, LEVEL, path.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Recursive CTE
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Rewrite the Demo 2a query (full org chart from the root, with LEVEL)
--       as a recursive CTE named org_chart with columns:
--         empno, ename, job, mgr, level_
--       The anchor selects employees where mgr IS NULL.
--       The recursive member joins emp to the CTE on emp.mgr = org_chart.empno.
--       Return all rows ordered by level_, empno.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Reporting Chain Length
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using CONNECT BY (not recursive CTE), write a query that shows
--       every employee alongside the total number of levels from that employee
--       up to the root (i.e., how many managers are above them, including
--       themselves — so root = 1, direct reports of root = 2, etc.).
--       Label this column chain_length.
--       Then filter to only employees whose chain_length = 3.
--       Return: empno, ename, job, deptno, chain_length.
--       Hint: use a full top-down CONNECT BY and use LEVEL AS chain_length.


-- YOUR CODE HERE
