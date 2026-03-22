/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
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
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- All UDFs and stored procedures in this lab are created in demo_db.demo_schema
-- to keep them isolated from the scott schema data objects.
-- ACCOUNTADMIN is used here to ensure warehouse access; in production,
-- SYSADMIN with appropriate grants is preferred.

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE DATABASE IF NOT EXISTS demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.demo_schema;

USE SCHEMA demo_db.demo_schema;

SELECT CURRENT_SCHEMA(), CURRENT_DATABASE();


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ SQL UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A SQL UDF wraps a single SQL expression and can be called anywhere a scalar
-- value is valid — SELECT list, WHERE clause, or as an argument to another function.
-- The function body is delimited by $$ (or single quotes); $$ is preferred
-- because it avoids escaping issues with single quotes inside the body.

CREATE OR REPLACE FUNCTION day_name_on(num_of_days INT)
RETURNS STRING
AS
$$
    SELECT 'In ' || CAST(num_of_days AS STRING) || ' days it will be a '
        || DAYNAME(DATEADD(DAY, num_of_days, CURRENT_DATE()))
$$;

SELECT DAY_NAME_ON(100);

SELECT
    days,
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) AS day_of_week,
    DAY_NAME_ON(days)                           AS udf_result
FROM (VALUES (100), (200), (300)) AS t(days);


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ JavaScript UDF and Overloading
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- JavaScript UDFs use the LANGUAGE JAVASCRIPT clause. Input parameters are
-- accessed as uppercase versions of their SQL names (NUM_OF_DAYS, not num_of_days).
-- UDF overloading allows the same function name with different parameter
-- signatures — Snowflake resolves the correct version at call time by matching
-- the number and types of arguments supplied.

-- 3a. Single-argument JavaScript UDF
CREATE OR REPLACE FUNCTION js_day_name_on(num_of_days FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    const weekday = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
    const date = new Date();
    date.setDate(date.getDate() + NUM_OF_DAYS);
    return 'In ' + NUM_OF_DAYS + ' days it will be a ' + weekday[date.getDay()];
$$;

SELECT JS_DAY_NAME_ON(100);

-- 3b. Overloaded version with abbreviation flag
CREATE OR REPLACE FUNCTION js_day_name_on(num_of_days FLOAT, is_abbr BOOLEAN)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    const weekday = IS_ABBR
        ? ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
        : ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
    const date = new Date();
    date.setDate(date.getDate() + NUM_OF_DAYS);
    return 'In ' + NUM_OF_DAYS + ' days it will be a ' + weekday[date.getDay()];
$$;

SELECT JS_DAY_NAME_ON(100, TRUE);
SELECT JS_DAY_NAME_ON(100, FALSE);


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Python UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Python UDFs require LANGUAGE PYTHON, a RUNTIME_VERSION, and a HANDLER
-- that names the Python function within the $$ body.
-- The handler function receives SQL argument values as Python native types.
-- Python UDFs run in a sandboxed Anaconda environment inside Snowflake —
-- no external network access and no package installation at runtime.

CREATE OR REPLACE FUNCTION py_day_name_on(num_of_days INT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'calculate_day_name'
AS
$$
def calculate_day_name(num_of_days):
    import datetime
    future_date = datetime.date.today() + datetime.timedelta(days=num_of_days)
    return f'In {num_of_days} days it will be a {future_date.strftime("%A")}'
$$;

SELECT PY_DAY_NAME_ON(100) AS py_dow;

SELECT
    days,
    JS_DAY_NAME_ON(days) AS js_result,
    PY_DAY_NAME_ON(days) AS py_result
FROM (VALUES (100), (200), (300)) AS t(days);


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ External Function (Illustrative — Requires API Integration)
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- External Functions call an HTTPS endpoint (AWS API Gateway, Azure, GCS) via
-- an API integration object. The code below is commented out because it
-- requires a live API integration to execute. The pattern shown is the
-- complete definition — students see the structure without running it.

/*
CREATE OR REPLACE API INTEGRATION demonstration_external_api_integration_01
    API_PROVIDER         = aws_api_gateway
    API_AWS_ROLE_ARN     = 'arn:aws:iam::123456789012:role/my_cloud_account_role'
    API_ALLOWED_PREFIXES = ('https://xyz.execute-api.us-west-2.amazonaws.com/production')
    ENABLED              = TRUE;

CREATE OR REPLACE EXTERNAL FUNCTION local_echo(string_col VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = demonstration_external_api_integration_01
    AS 'https://xyz.execute-api.us-west-2.amazonaws.com/production/remote_echo';

SELECT local_echo('hello');
*/


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ JavaScript Stored Procedure — Dynamic SQL
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Stored procedures differ from UDFs in two key ways:
--   1. They are called with CALL, not used in SELECT expressions.
--   2. They can execute DDL and DML — UDFs cannot.
-- The JavaScript API uses snowflake.execute({sqlText: ...}) to run SQL
-- statements dynamically. The result is iterated with result_set.next().

CREATE OR REPLACE SCHEMA demo_db.tmp_schema;

USE SCHEMA demo_db.tmp_schema;

CREATE TABLE IF NOT EXISTS demo_table1 (name STRING, age INT);
CREATE OR REPLACE TABLE demo_table2  (name STRING, age INT);

INSERT INTO demo_table1 VALUES ('Joe',51),('Tom',33),('Clark',52),('Ruth',40);
INSERT INTO demo_table2 VALUES ('Joe',51),('Tom',33),('Clark',52),('Ruth',40);

SELECT COUNT(*) AS row_count FROM demo_table1;
SELECT COUNT(*) AS row_count FROM demo_table2;

CREATE OR REPLACE PROCEDURE truncate_all_tables_in_schema(
    database_name STRING,
    schema_name   STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var result = [];
    var namespace = DATABASE_NAME + '.' + SCHEMA_NAME;
    var result_set = snowflake.execute({sqlText: 'SHOW TABLES IN ' + namespace});
    while (result_set.next()) {
        var table_name = result_set.getColumnValue(2);
        snowflake.execute({sqlText: 'TRUNCATE TABLE ' + table_name});
        result.push(namespace + '.' + table_name + ' truncated.');
    }
    return result.join('\n');
$$;

SHOW TABLES IN SCHEMA DEMO_DB.TMP_SCHEMA;

CALL truncate_all_tables_in_schema('DEMO_DB', 'TMP_SCHEMA');

SELECT COUNT(*) AS row_count FROM demo_table1;
SELECT COUNT(*) AS row_count FROM demo_table2;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 7 │ Stored Procedure with Return Value — RESULT_SCAN Chaining
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A stored procedure's return value is accessible via RESULT_SCAN after CALL.
-- Two column reference styles work:
--   Named:      SELECT concat_strings FROM TABLE(RESULT_SCAN(...))
--   Positional: SELECT $1             FROM TABLE(RESULT_SCAN(...))
-- The positional form is safer when the procedure name or return column name
-- contains special characters or changes between versions.

CREATE OR REPLACE PROCEDURE concat_strings(s1 STRING, s2 STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    return S1 + S2;
$$;

CALL concat_strings('abc-', 'xyz');

SET qid = (SELECT LAST_QUERY_ID());

-- Named column reference
SELECT *
FROM TABLE(RESULT_SCAN($qid));

SET result = (SELECT concat_strings FROM TABLE(RESULT_SCAN($qid)));
SELECT $result;

-- Chain the result into a second call
CALL concat_strings($result, '-ddd');

-- Compact chaining using LAST_QUERY_ID inline
CALL concat_strings('abc-', 'xyz');
SET result = (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
CALL concat_strings($result, '-ddd');


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 8 │ SQL Scripting SP — Returning a TABLE Result Set
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- SQL Scripting stored procedures (LANGUAGE SQL) can declare a RESULTSET and
-- return it as a TABLE — making the output queryable in a FROM clause.
-- Three equivalent ways to consume the result:
--   1. CALL — shows results as a table in the worksheet
--   2. SELECT * FROM TABLE(sp_name(...)) — inline in a query
--   3. CALL ... ->> SELECT * FROM $1   — Flow Pipe operator

CREATE OR REPLACE PROCEDURE get_sales_sp(target_region STRING)
RETURNS TABLE (id INT, product STRING, price NUMBER(10,2), region STRING)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT * FROM (
            SELECT 1, 'Snowflake Pro License',      1200.00, 'North America'
            UNION ALL
            SELECT 2, 'Data Engineering Course',     450.00, 'EMEA'
            UNION ALL
            SELECT 3, 'Cloud Storage Add-on',        150.50, 'North America'
            UNION ALL
            SELECT 4, 'Consulting Session',         3000.00, 'APAC'
        ) AS sales(transaction_id, product_name, amount, region_name)
        WHERE region_name = :target_region OR :target_region = 'ALL'
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- 8a. CALL
CALL get_sales_sp('ALL');

-- 8b. TABLE() function in FROM clause
SELECT *
FROM TABLE(get_sales_sp('North America'));

-- 8c. Flow Pipe operator
CALL get_sales_sp('ALL') ->> SELECT * FROM $1;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 9 │ User Defined Table Function (UDTF)
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A UDTF returns a TABLE rather than a scalar value. It is called with
-- TABLE(function_name(args)) in the FROM clause — never with CALL.
-- UDTFs are useful for parameterised data generation or row-producing logic
-- that would otherwise require a stored procedure or temp table.

CREATE OR REPLACE FUNCTION get_demo_sales_data(target_region STRING)
RETURNS TABLE (
    transaction_id INT,
    product_name   STRING,
    amount         NUMBER(10,2),
    region_name    STRING
)
AS
$$
    SELECT * FROM (
        SELECT 1, 'Snowflake Pro License',  1200.00, 'North America'
        UNION ALL
        SELECT 2, 'Data Engineering Course', 450.00, 'EMEA'
        UNION ALL
        SELECT 3, 'Cloud Storage Add-on',    150.50, 'North America'
        UNION ALL
        SELECT 4, 'Consulting Session',     3000.00, 'APAC'
    ) AS sales(transaction_id, product_name, amount, region_name)
    WHERE region_name = target_region OR target_region = 'ALL'
$$;

SELECT *
FROM TABLE(get_demo_sales_data('North America'))
WHERE amount > 500;

SELECT *
FROM TABLE(get_demo_sales_data('ALL'));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- demo_db must NOT be dropped — it is used in all remaining Day 2 and Day 4 labs.

DROP SCHEMA IF EXISTS demo_db.tmp_schema;

-- DROP DATABASE IF EXISTS demo_db;   -- keep: used in all remaining labs


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create objects in demo_db.demo_schema.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ SQL UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a SQL UDF called salary_band(sal FLOAT) that returns a STRING:
--         'HIGH'   if sal >= 3000
--         'MEDIUM' if sal >= 1500
--         'LOW'    otherwise
--       Then call it in a SELECT against demo_db.scott.emp, returning:
--         ename, sal, salary_band(sal) AS band
--       ordered by sal DESC.

USE SCHEMA demo_db.demo_schema;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Python UDF
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a Python UDF called initcap_name(full_name STRING) that returns
--       the input string in title case (first letter of each word capitalised).
--       Use Python's str.title() method.
--       Test it with: SELECT initcap_name('JOHN SMITH'), initcap_name('alice jones');


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Stored Procedure with Return Value
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a JavaScript stored procedure called format_greeting(name STRING)
--       that returns the string 'Hello, <name>! Welcome to Snowflake.'
--       Then:
--         A) CALL the procedure with your own name.
--         B) Capture the result into a session variable using SET + RESULT_SCAN.
--         C) CALL it again, passing $result as the name argument so the output
--            becomes a nested greeting.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — UDTF + Flow Pipe
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a SQL Scripting stored procedure called get_emp_by_dept(
--           dept_no INT)
--       that returns TABLE(empno INT, ename STRING, job STRING, sal FLOAT)
--       by querying demo_db.scott.emp WHERE deptno = :dept_no.
--       Then call it three ways:
--         A) CALL get_emp_by_dept(10)
--         B) SELECT * FROM TABLE(get_emp_by_dept(20))
--         C) CALL get_emp_by_dept(30) ->> SELECT ename, sal FROM $1
--            ORDER BY sal DESC


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop only objects created in this exercise set.
-- Do NOT drop demo_db — used in all remaining labs.

DROP FUNCTION  IF EXISTS demo_db.demo_schema.salary_band(FLOAT);
DROP FUNCTION  IF EXISTS demo_db.demo_schema.initcap_name(STRING);
DROP PROCEDURE IF EXISTS demo_db.demo_schema.format_greeting(STRING);
DROP PROCEDURE IF EXISTS demo_db.demo_schema.get_emp_by_dept(INT);
