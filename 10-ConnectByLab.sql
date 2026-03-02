/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) CONNECT BY clause
2) START WITH clause
3) LEVEL pseudo-column
4) CONNECT_BY_ROOT function
5) Recursive CTEs
6) Range Generators
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE DATABASE demo_db;
USE SCHEMA scott;


-- Step 2 – Review manager/employee relationship in emp table

SELECT
    empno,
    ename,
    job,
    mgr
FROM emp;

SELECT
    e.empno,
    e.ename,
    e.job,
    e.mgr,
    m.ename AS mgr_name,
    m.job   AS mgr_title
FROM emp e
LEFT JOIN emp m ON e.mgr = m.empno;


-- Step 3 – List all employees alongside their respective managers

-- Strategy #1: Using UNION ALL and NOT EXISTS
SELECT
    e.empno,
    e.ename,
    e.job,
    e.mgr,
    m.ename AS mgr_name,
    m.job   AS mgr_title
FROM emp e
JOIN emp m ON e.mgr = m.empno
UNION ALL
SELECT
    e.empno,
    e.ename,
    e.job,
    NULL,
    NULL,
    NULL
FROM emp e
WHERE NOT EXISTS (
    SELECT 1
    FROM emp x
    WHERE NVL(e.mgr, 0) = x.empno
);
--WHERE mgr IS NULL;


-- Strategy #2: Using LEFT JOIN
SELECT
    e.empno,
    e.ename,
    e.job,
    e.mgr,
    m.ename AS mgr_name,
    m.job   AS mgr_title
FROM emp e
LEFT JOIN emp m ON e.mgr = m.empno;


-- Strategy #3: Using Hierarchical Query with CONNECT BY

-- 1. Basic hierarchy
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno;

-- 2. With path and indented name
SELECT
    empno,
    LPAD('.', (LEVEL-1)*5, '.') || ename name,
    job,
    mgr,
    LEVEL,
    LTRIM(SYS_CONNECT_BY_PATH(empno, '>'), '>') path
FROM emp
START WITH mgr IS NULL
CONNECT BY mgr = PRIOR empno
ORDER BY path;

-- 3. Using CTE to separate hierarchy from ordering
WITH x AS (
    SELECT
        empno,
        LPAD('.', (LEVEL-1)*5, '.') || ename empl_name,
        job,
        mgr,
        LEVEL AS empl_level,
        LTRIM(SYS_CONNECT_BY_PATH(empno, '>'), '>') path
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

-- 4. CTE with explicit column names
WITH x(empno, empl_name, job, mgr, empl_level, path) AS (
    SELECT
        empno,
        LPAD('.', (LEVEL-1)*5, '.') || ename,
        job,
        mgr,
        LEVEL AS lvl,
        LTRIM(SYS_CONNECT_BY_PATH(empno, '>'), '>') path
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


-- Step 4 – Verify SYS_CONNECT_BY_PATH availability

SHOW FUNCTIONS;

SHOW FUNCTIONS;
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'SYS_CONNECT_BY_PATH';

SELECT * FROM emp;


-- Step 5 – More examples using CONNECT BY and START WITH

-- 1. Descendants of employee 7788
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7788
CONNECT BY mgr = PRIOR empno;

-- 2. Ancestors of employee 7788
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7788
CONNECT BY PRIOR mgr = empno;

-- 3. Ancestors with direct manager using LEAD
WITH x AS (
    SELECT
        empno,
        ename,
        job,
        mgr,
        LEVEL
    FROM emp
    START WITH empno = 7788
    CONNECT BY PRIOR mgr = empno
)
SELECT
    x.*,
    LEAD(ename, 1) OVER(ORDER BY NULL) AS direct_manager
FROM x;

-- CTE = COMMON TABLE EXPRESSION

-- 4. Descendants of employee 7788 (forward direction)
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7788
CONNECT BY mgr = PRIOR empno;


-- Step 6 – CONNECT_BY_ROOT: multiple starting points

-- 1. Multiple roots with CONNECT_BY_ROOT
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL,
    CONNECT_BY_ROOT(empno) root_empno
FROM emp
START WITH empno IN (7369, 7499)
CONNECT BY PRIOR mgr = empno
ORDER BY root_empno, LEVEL;

-- A >> B >> C >> D >> E >> F

-- 2. Equivalent using UNION ALL
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7369
CONNECT BY PRIOR mgr = empno
UNION ALL
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL
FROM emp
START WITH empno = 7499
CONNECT BY PRIOR mgr = empno;

-- 3. Without START WITH (all paths)
SELECT
    empno,
    ename,
    job,
    mgr,
    LEVEL,
    CONNECT_BY_ROOT(empno) root_empno
FROM emp
--START WITH empno IN (7369, 7499)
CONNECT BY PRIOR mgr = empno
ORDER BY root_empno, LEVEL;


-- Step 7 – Recursive CTEs

-- Regular CTE (explicit column list)
WITH x(id, name, title) AS (
    SELECT empno, ename, job
    FROM emp
    WHERE deptno = 10
)
SELECT *
FROM x;

-- Regular CTE (no column list — compare column names)
WITH x AS (
    SELECT empno, ename, job
    FROM emp
    WHERE deptno = 10
)
SELECT *
FROM x;


-- Step 8 – Recursive CTE vs CONNECT BY (intersection proves equivalence)

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
        JOIN x ON e.mgr = x.empno    -- mgr = PRIOR empno
    )
    SELECT *
    FROM x
);


-- Recursive CTE standalone
WITH x(empno, ename, job, mgr, level_) AS (
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
