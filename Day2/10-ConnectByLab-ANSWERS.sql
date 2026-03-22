/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Self-join and UNION ALL for hierarchical data
2) CONNECT BY clause and START WITH
3) LEVEL pseudo-column and SYS_CONNECT_BY_PATH
4) CONNECT_BY_ROOT — multiple starting points
5) Recursive CTEs — WITH RECURSIVE
6) CONNECT BY vs Recursive CTE equivalence
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Basic CONNECT BY
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Descendants of JONES with LEVEL.

USE DATABASE demo_db;
USE SCHEMA scott;

SELECT
    empno,
    ename,
    job,
    LEVEL
FROM emp
START WITH ename = 'JONES'
CONNECT BY mgr = PRIOR empno;

-- [TEACHING NOTE]
-- JONES (empno 7566) is a MANAGER who directly manages SCOTT (7788) and FORD (7902).
-- SCOTT manages ADAMS (7876); FORD manages SMITH (7369).
-- The subtree therefore has 3 levels: JONES (1), SCOTT/FORD (2), ADAMS/SMITH (3).
-- Discussion point: what does LEVEL = 1 represent when START WITH ename = 'JONES'?
-- (Answer: JONES himself — LEVEL 1 is the starting node, not his manager.)
-- Common mistake: using START WITH mgr = JONES's empno instead of START WITH
-- ename = 'JONES' — the former would start with JONES's direct reports (level 1
-- = SCOTT/FORD), completely omitting JONES from the result.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Ancestor Path with SYS_CONNECT_BY_PATH
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Walk UP from ADAMS (7876) to root, build ename path.

SELECT
    empno,
    ename,
    job,
    LEVEL,
    LTRIM(SYS_CONNECT_BY_PATH(ename, ' -> '), ' -> ') AS path
FROM emp
START WITH empno = 7876
CONNECT BY PRIOR mgr = empno;

-- [TEACHING NOTE]
-- Walking upward: CONNECT BY PRIOR mgr = empno means "the current row's empno
-- equals the previous row's mgr" — each step moves one level up the org chart.
-- Starting from ADAMS, the path is:  ADAMS -> SCOTT -> JONES -> KING
-- LTRIM removes the leading ' -> ' separator added by SYS_CONNECT_BY_PATH.
-- Common mistake: confusing CONNECT BY mgr = PRIOR empno (downward) with
-- CONNECT BY PRIOR mgr = empno (upward) — the position of PRIOR determines
-- the traversal direction.
-- Early finishers: modify the query to include the manager's job title
-- by joining back to emp on PRIOR empno = mgr.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Recursive CTE
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Full org chart as recursive CTE, ordered by level_, empno.

WITH RECURSIVE org_chart(empno, ename, job, mgr, level_) AS (
    -- Anchor: root nodes (president)
    SELECT empno, ename, job, mgr, 1
    FROM emp
    WHERE mgr IS NULL
    UNION ALL
    -- Recursive member: join children to their parent
    SELECT e.empno, e.ename, e.job, e.mgr, org_chart.level_ + 1
    FROM emp e
    JOIN org_chart ON e.mgr = org_chart.empno
)
SELECT *
FROM org_chart
ORDER BY level_, empno;

-- [TEACHING NOTE]
-- The anchor selects the single root (KING, mgr IS NULL).
-- Each recursive step finds employees whose mgr = a previously visited empno.
-- The CTE terminates when no new rows satisfy the JOIN condition (leaf nodes).
-- The result is identical to Demo 5a — this exercise proves the equivalence
-- without using the INTERSECT approach.
-- Common mistake: omitting WITH RECURSIVE and using just WITH — Snowflake
-- requires the RECURSIVE keyword for self-referencing CTEs; without it,
-- the forward reference to org_chart in the recursive member is an error.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Reporting Chain Length
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Show all employees with chain_length, filter to chain_length = 3.

SELECT
    empno,
    ename,
    job,
    deptno,
    LEVEL AS chain_length
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno 
->>
SELECT * 
FROM $1
WHERE chain_length = 3
ORDER BY deptno, empno;

