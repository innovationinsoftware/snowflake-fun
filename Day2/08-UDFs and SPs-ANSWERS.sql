/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) SQL User Defined Functions (UDFs)
2) JavaScript UDFs and UDF overloading
3) Python UDFs
4) External Functions (illustrative — requires API integration)
5) Stored Procedures (JavaScript) — executing dynamic SQL
6) Stored Procedures with return values and RESULT_SCAN chaining
7) SQL Scripting Stored Procedures returning TABLE result sets
8) User Defined Table Functions (UDTFs)
9) Flow Pipe Operator (->> ) with stored procedures
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ SQL UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create salary_band(sal FLOAT) and call it against demo_db.scott.emp.

USE SCHEMA demo_db.demo_schema;

CREATE OR REPLACE FUNCTION salary_band(sal FLOAT)
RETURNS STRING
AS
$$
    SELECT CASE
        WHEN sal >= 3000 THEN 'HIGH'
        WHEN sal >= 1500 THEN 'MEDIUM'
        ELSE 'LOW'
    END
$$;

SELECT
    ename,
    sal,
    salary_band(sal) AS band
FROM demo_db.scott.emp
ORDER BY sal DESC;

-- [TEACHING NOTE]
-- The UDF body is a single SELECT expression — it cannot contain multiple
-- statements. Complex multi-branch logic uses CASE inside the SELECT.
-- The UDF is created in demo_schema but called against demo_db.scott.emp —
-- three-part qualification lets the function and the data live in different schemas.
-- Discussion point: what is the difference between a UDF and a computed column?
-- (Answer: a computed column is stored metadata — it is always derived at read time.
--  A UDF is callable code — it can be parameterised, versioned, reused across
--  many tables, and granted to roles independently.)
-- Common mistake: calling salary_band(sal) without qualifying the function name
-- when the current schema is different from demo_schema — always use the
-- fully qualified name: demo_db.demo_schema.salary_band(sal).


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Python UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create initcap_name(full_name STRING) using Python str.title().

CREATE OR REPLACE FUNCTION initcap_name(full_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'to_title'
AS
$$
def to_title(full_name):
    return full_name.title()
$$;

SELECT initcap_name('JOHN SMITH')  AS result_1;
SELECT initcap_name('alice jones') AS result_2;

-- [TEACHING NOTE]
-- Python's str.title() capitalises the first character of each word and
-- lowercases the rest — which is exactly what the SQL INITCAP function does.
-- The exercise illustrates that Python UDFs can replicate built-in SQL functions,
-- but the real value is in logic that SQL cannot express natively (e.g. regex,
-- external library calls, complex branching).
-- Common mistake: naming the Python function the same as the SQL function
-- ('initcap_name') — Snowflake uses the HANDLER name to locate the Python
-- entry point, so they can differ. The handler name in $$ must match exactly.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Stored Procedure with Return Value
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create format_greeting, chain its output into a second call.

CREATE OR REPLACE PROCEDURE format_greeting(name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    return 'Hello, ' + NAME + '! Welcome to Snowflake.';
$$;

-- Task A: initial call
CALL format_greeting('Alex');

-- Task B: capture result
SET result = (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
SELECT $result;

-- Task C: nested greeting — pass the returned string as the name
CALL format_greeting($result);

-- [TEACHING NOTE]
-- The positional $1 reference is used instead of a named column reference
-- because the column name after CALL is the procedure name itself
-- ('format_greeting') — using $1 is safer and more readable.
-- The nested greeting in Task C produces a string like:
--   "Hello, Hello, Alex! Welcome to Snowflake.! Welcome to Snowflake."
-- This demonstrates that SP return values are plain strings — they can be
-- used as inputs to any subsequent call.
-- Common mistake: running SELECT $result before SET result is evaluated —
-- $result is a session variable, not a SQL expression; it must be SET first.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — UDTF + Flow Pipe
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create get_emp_by_dept(dept_no INT) returning TABLE, call three ways.

CREATE OR REPLACE PROCEDURE get_emp_by_dept(dept_no INT)
RETURNS TABLE (empno INT, ename STRING, job STRING, sal FLOAT)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT empno, ename, job, sal
        FROM demo_db.scott.emp
        WHERE deptno = :dept_no
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- Task A: CALL
CALL get_emp_by_dept(10);

-- Task B: TABLE() in FROM
SELECT *
FROM TABLE(get_emp_by_dept(20));

-- Task C: Flow Pipe with ORDER BY
CALL get_emp_by_dept(30) ->> SELECT ename, sal FROM $1 ORDER BY sal DESC;

-- [TEACHING NOTE]
-- The three consumption patterns are interchangeable for read access but differ
-- in composability: TABLE() can be joined, filtered, and aggregated in a FROM
-- clause; CALL is standalone; ->> is concise for ad-hoc transformations.
-- In production, TABLE() is the most flexible pattern for SP-as-data-source.
-- Common mistake: trying to use WHERE deptno = dept_no (unbound parameter)
-- instead of WHERE deptno = :dept_no (Snowflake Scripting bind syntax).
-- The colon prefix is required for binding SP parameters inside a DECLARE block.
-- Early finishers: add an ORDER BY empno to the RESULTSET inside the procedure
-- so the result is always sorted — then verify all three calling patterns
-- return rows in empno order.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

DROP FUNCTION  IF EXISTS demo_db.demo_schema.salary_band(FLOAT);
DROP FUNCTION  IF EXISTS demo_db.demo_schema.initcap_name(STRING);
DROP PROCEDURE IF EXISTS demo_db.demo_schema.format_greeting(STRING);
DROP PROCEDURE IF EXISTS demo_db.demo_schema.get_emp_by_dept(INT);
