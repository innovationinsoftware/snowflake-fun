/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Analytic Functions
2) QUALIFY clause
3) Advanced SQL Puzzles
----------------------------------------------------------------------------------*/

use schema demo_db.scott;

-- Step 1 – Get the top 1 row: traditional approaches

-- 1. Using TOP option
SELECT TOP 1 *
FROM emp
ORDER BY hiredate;

-- 2. Using LIMIT clause
SELECT *
FROM emp
ORDER BY hiredate
LIMIT 1;

-- 3. Using CTE with ROW_NUMBER
WITH x AS (
    SELECT *, ROW_NUMBER() OVER(ORDER BY hiredate) rn
    FROM emp
)
SELECT *
FROM x
WHERE rn = 1;

-- 4. Using ROW_NUMBER with QUALIFY
SELECT *, ROW_NUMBER() OVER(ORDER BY hiredate) rn
FROM emp
QUALIFY rn = 1;

-- 5. Inline QUALIFY
SELECT *
FROM emp
QUALIFY ROW_NUMBER() OVER(ORDER BY hiredate) = 1;


-- Step 2 – Challenge #1: Identify all employees hired first (or tied for first) in each department

-- Strategy #1: Using CTE

-- 1. Find MIN hiredate in each department
SELECT
    deptno,
    MIN(hiredate) first_hiredate
FROM emp
GROUP BY deptno
ORDER BY 1;

-- 2. Find employees hired on those specific dates
WITH x AS (
    SELECT
        deptno,
        MIN(hiredate) first_hiredate
    FROM emp
    GROUP BY deptno
)
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND e.hiredate = x.first_hiredate
ORDER BY e.deptno;


-- Step 3 – Challenge #2: Identify all employees hired first or last in each department

-- Strategy #1: Using CTE with IN clause
WITH x AS (
    SELECT
        deptno,
        MIN(hiredate) first_hiredate,
        MAX(hiredate) last_hiredate
    FROM emp
    GROUP BY deptno
)
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND (e.hiredate = x.first_hiredate OR e.hiredate = x.last_hiredate)
ORDER BY e.deptno, e.hiredate;

-- Alternative syntax
WITH x AS (
    SELECT
        deptno,
        MIN(hiredate) first_hiredate,
        MAX(hiredate) last_hiredate
    FROM emp
    GROUP BY deptno
)
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND e.hiredate IN (x.first_hiredate, x.last_hiredate)
ORDER BY e.deptno, e.hiredate;

-- Strategy #2: Using CTE and UNION ALL
WITH x AS (
    SELECT
        deptno,
        MIN(hiredate) first_hiredate,
        MAX(hiredate) last_hiredate
    FROM emp
    GROUP BY deptno
)
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND e.hiredate = x.first_hiredate
UNION ALL
SELECT e.*
FROM emp e
JOIN x ON e.deptno = x.deptno AND e.hiredate = x.last_hiredate
ORDER BY deptno, hiredate;

-- Strategy #3: Using MIN/MAX Analytic functions

-- 1. Show first and last department hire dates next to each employee's data
SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno,
    MIN(hiredate) OVER(PARTITION BY deptno) first_date,
    MAX(hiredate) OVER(PARTITION BY deptno) last_date
FROM emp
ORDER BY deptno;

-- 2. Apply filter on hiredate column
WITH x AS (
    SELECT
        empno,
        ename,
        job,
        hiredate,
        deptno,
        MIN(hiredate) OVER(PARTITION BY deptno) first_date,
        MAX(hiredate) OVER(PARTITION BY deptno) last_date
    FROM emp
)
SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno,
    first_date first_hiredate,
    last_date  last_hiredate
FROM x
WHERE hiredate IN (first_date, last_date)
ORDER BY deptno, hiredate;

-- Strategy #4: Using MIN/MAX Analytic functions and QUALIFY clause
SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno,
    MIN(hiredate) OVER(PARTITION BY deptno) first_hiredate,
    MAX(hiredate) OVER(PARTITION BY deptno) last_hiredate
FROM emp
QUALIFY hiredate IN (first_hiredate, last_hiredate)
ORDER BY deptno, hiredate;

-- Improved query
SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno
FROM emp
QUALIFY hiredate IN (
    MIN(hiredate) OVER(PARTITION BY deptno),
    MAX(hiredate) OVER(PARTITION BY deptno)
)
ORDER BY deptno, hiredate;


-- Step 4 – Simulating duplicates

INSERT INTO scott.emp VALUES
(7782,'WILSON','MANAGER',7839,TO_DATE('09-06-1981','dd-mm-yyyy'),2450,NULL,10),
(7783,'POOJA', 'MANAGER',7839,TO_DATE('09-06-1981','dd-mm-yyyy'),2450,NULL,10);

-- Strategy #5: Using ROW_NUMBER function
SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno
FROM emp
QUALIFY ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY hiredate, empno ASC) = 1
ORDER BY deptno;

DELETE FROM scott.emp
WHERE ename IN ('WILSON','POOJA');


-- Step 5 – Demonstration: Using multiple analytic functions in the same query

SELECT
    empno,
    ename,
    job,
    hiredate,
    deptno,
    sal,
    --MIN(hiredate) OVER(PARTITION BY deptno) first_date,
    --ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY hiredate) rn,
    --RANK() OVER(PARTITION BY deptno ORDER BY hiredate) rk,
    --MIN(hiredate) OVER(PARTITION BY job) first_date_job,
    --ROW_NUMBER() OVER(PARTITION BY job ORDER BY hiredate) rn_job,
    --RANK() OVER(PARTITION BY job ORDER BY hiredate) rk_job,
    MIN(sal) OVER(PARTITION BY job)        min_sal_job,
    ROW_NUMBER() OVER(PARTITION BY job ORDER BY sal) rn_job_sal,
    RANK() OVER(ORDER BY sal)              rk_job,
    DENSE_RANK() OVER(ORDER BY sal)        drk_job,
    COUNT(*) OVER(PARTITION BY deptno)     dept_count,
    COUNT(*) OVER()                        total_count
FROM emp
ORDER BY sal;


-- Step 6 – Advanced Challenges

-- Challenge #3: Employees in the Same Department as the President(s)

/*
    Objective:
        Write a query to find all employees who work in the same department(s) as the president(s).

    Requirements:
        Your query must work even if there are multiple "PRESIDENT" records in the emp table.
        Ensure that Snowflake scans the emp table only once for efficiency.
*/

-- 1. Show president count per department alongside all employees
SELECT
    *,
    COUNT(CASE WHEN job = 'PRESIDENT' THEN 1 END) OVER(PARTITION BY deptno) num_of_presidents
FROM emp
ORDER BY deptno;

-- 2. Filter to only departments that have at least one president (QUALIFY)
SELECT *
FROM emp
QUALIFY COUNT(CASE WHEN job = 'PRESIDENT' THEN 1 END) OVER(PARTITION BY deptno) > 0
ORDER BY deptno;

-- 3. Alternative using SUM
SELECT *
FROM emp
QUALIFY SUM(CASE WHEN job = 'PRESIDENT' THEN 1 END) OVER(PARTITION BY deptno) > 0
ORDER BY deptno;

-- 4. Correlated subquery approach
SELECT *
FROM emp a
WHERE 0 < (
    SELECT COUNT(*)
    FROM emp
    WHERE job = 'PRESIDENT'
    AND deptno = a.deptno
);


-- Challenge #4: Employees in the Department of the Top-Paid Clerk

/*
    Objective:
        Write a query to find all employees who work in the same department as the highest-paid "CLERK."

    Requirements:
        Ensure your query handles ties (i.e., if there are multiple top-paid clerks in different departments).
*/

SELECT *
FROM emp a
QUALIFY MAX(CASE WHEN job = 'CLERK' THEN sal END) OVER(PARTITION BY deptno) =
        MAX(CASE WHEN job = 'CLERK' THEN sal END) OVER()
ORDER BY deptno;


-- Challenge #5: Employees Paid Above the Department Average

/*
    Objective:
        Write a query to find all employees whose salary is above the average salary
        of their respective department.
*/

-- 1. Correlated subquery
SELECT *
FROM emp e
WHERE sal > (
    SELECT AVG(sal)
    FROM emp
    WHERE deptno = e.deptno
)
ORDER BY deptno;

-- 2. Analytic function with QUALIFY
SELECT * --, AVG(sal) OVER(PARTITION BY deptno) avg_sal
FROM emp e
QUALIFY sal > AVG(sal) OVER(PARTITION BY deptno)
ORDER BY deptno, AVG(sal) OVER(PARTITION BY deptno);


-- Challenge #6: Employees with the Same Department and Job Title as ADAMS

/*
    Objective:
        Write a query to list all employees who work in the same department and hold
        the same job title as the employee named "ADAMS."
*/

-- 1. Using subquery with row-value comparison
SELECT *
FROM emp
WHERE (deptno, job) IN (
    SELECT deptno, job
    FROM emp
    WHERE ename = 'ADAMS'
);

-- 2. Using CTE with JOIN
WITH x AS (
    SELECT deptno, job
    FROM emp
    WHERE ename = 'ADAMS'
)
SELECT *
FROM emp
JOIN x ON emp.deptno = x.deptno AND emp.job = x.job;

-- 3. Using QUALIFY
SELECT *
FROM emp
QUALIFY COUNT(CASE ename WHEN 'ADAMS' THEN 1 END) OVER(PARTITION BY deptno, job) > 0
ORDER BY deptno, job;
