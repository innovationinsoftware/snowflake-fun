/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) DATA_RETENTION_TIME_IN_DAYS — account, database, schema, and table levels
2) Retention time inheritance and override hierarchy
3) UNDROP — recovering dropped tables
4) SHOW TABLES HISTORY — visibility of dropped objects
5) Time Travel queries — AT(OFFSET), AT(STATEMENT), AT(TIMESTAMP), BEFORE(STATEMENT)
6) Restoring data from Time Travel into a new table
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup and Working Table
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A clone of demo_db.scott.dept is created to use as a mutable working copy.
-- The source scott.dept is left untouched throughout this lab.
-- SHOW SCHEMAS confirms demo_schema exists before use.

USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS demo_db;
USE DATABASE demo_db;

SHOW SCHEMAS LIKE 'DEMO_SCHEMA';

CREATE SCHEMA IF NOT EXISTS demo_schema;
USE SCHEMA demo_schema;

SHOW TABLES;

CREATE OR REPLACE TABLE dept_copy CLONE demo_db.scott.dept;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Retention Time — Hierarchy and Override
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- DATA_RETENTION_TIME_IN_DAYS can be set at four levels:
--   account → database → schema → table
-- Each level inherits from its parent but can be overridden independently.
-- Setting a schema to 0 effectively disables Time Travel for all tables in it
-- unless the table itself has an explicit override.
-- Discussion point: "What happens to a table's retention if its parent schema
-- is set to 0 but the table itself was set to 5?"
-- (Answer: the table keeps its own value of 5 — table-level overrides the schema.)

-- 2a. Baseline — default is 1 day
SHOW DATABASES LIKE 'DEMO_DB';

-- 2b. Raise account-level retention to 90 days
ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS = 90;
SHOW DATABASES LIKE 'DEMO_DB';

-- 2c. Set database-level to 45 days
ALTER DATABASE demo_db SET DATA_RETENTION_TIME_IN_DAYS = 45;
SHOW DATABASES LIKE 'DEMO_DB';

-- 2d. Inspect inheritance at schema and table level
SHOW SCHEMAS LIKE 'DEMO_SCHEMA';
SHOW SCHEMAS;
SHOW TABLES LIKE 'dept_copy';

-- 2e. Override at schema and table level
ALTER SCHEMA demo_schema SET DATA_RETENTION_TIME_IN_DAYS = 10;
ALTER TABLE dept_copy SET DATA_RETENTION_TIME_IN_DAYS = 5;

-- 2f. Setting schema to 0 disables Time Travel for that schema
ALTER SCHEMA demo_schema SET DATA_RETENTION_TIME_IN_DAYS = 0;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ UNDROP — Recovering a Dropped Table
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- SHOW TABLES HISTORY displays both live and dropped tables (within retention).
-- The "dropped_on" column is NULL for live tables and populated for dropped ones.
-- UNDROP restores the most recently dropped version of the table.
-- After UNDROP, "dropped_on" returns to NULL in SHOW TABLES HISTORY.

-- 3a. Baseline
SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 3b. Drop the table
DROP TABLE dept_copy;

SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 3c. Restore it
UNDROP TABLE dept_copy;

SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM dept_copy;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Time Travel Queries — AT and BEFORE
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Three time travel reference types:
--   AT(OFFSET => -N)        — N seconds before now (relative)
--   AT(STATEMENT => $qid)   — inclusive of all changes made by that statement
--   AT(TIMESTAMP => ...)    — at a specific wall-clock time
--   BEFORE(STATEMENT => $qid) — up to but NOT including that statement's changes
-- AT and BEFORE can be used in any SELECT, including CTEs and subqueries.

-- 4a. Truncate the working table and capture the query ID
TRUNCATE TABLE dept_copy;

SET trunc_qid = (SELECT LAST_QUERY_ID());

SELECT * FROM dept_copy;

-- 4b. Offset-based: 1 minute ago
SELECT *
FROM dept_copy
AT(OFFSET => -60 * 1);

-- 4c. AT(STATEMENT) — inclusive of the TRUNCATE (returns empty — data was removed)
SELECT *
FROM dept_copy
AT(STATEMENT => $trunc_qid);

-- 4d. BEFORE(STATEMENT) — excludes the TRUNCATE (returns original rows)
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

-- 4e. Timestamp-based
SELECT DATEADD(minute, -2, CURRENT_TIMESTAMP());

SELECT *
FROM dept_copy
AT(TIMESTAMP => DATEADD(minute, -2, CURRENT_TIMESTAMP()));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Restoring Data from Time Travel
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The standard restore pattern is: CREATE TABLE ... AS SELECT ... BEFORE(...)
-- This materialises the historical snapshot as a new permanent table.
-- UNDROP is an alternative but only works while the table still exists
-- within its retention window — CTAS from Time Travel is more flexible.

-- 5a. Restore into a new table
CREATE OR REPLACE TABLE dept_copy_restored AS
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

SELECT * FROM dept_copy_restored;

-- 5b. DROP, then UNDROP to show the alternative restore path
DROP TABLE dept_copy;
SHOW TABLES HISTORY;

UNDROP TABLE dept_copy;

SELECT * FROM dept_copy;

-- 5c. Overwrite dept_copy with pre-truncate data using CTAS
DROP TABLE dept_copy;

UNDROP TABLE dept_copy;

CREATE OR REPLACE TABLE dept_copy AS
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

SHOW TABLES HISTORY;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Reset account retention to the default of 1 day after the demo.
-- demo_db must NOT be dropped — it is used in all remaining Day 2 and Day 4 labs.

ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS = 1;
-- DROP DATABASE demo_db;   -- keep: used in all remaining labs


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises use objects inside demo_db.demo_schema.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Retention Time Settings
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: In demo_db.demo_schema, create a table called emp_copy as a clone of
--       demo_db.scott.emp.
--       A) Check the current retention_time for emp_copy using SHOW TABLES.
--       B) Set emp_copy's retention to 3 days using ALTER TABLE.
--       C) Confirm the change by running SHOW TABLES LIKE 'emp_copy' and
--          reading the "retention_time" column via RESULT_SCAN.

USE DATABASE demo_db;
USE SCHEMA demo_schema;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ UNDROP
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Drop emp_copy, then use SHOW TABLES HISTORY to confirm it shows as
--       dropped (dropped_on is not null).  Then UNDROP it and verify it is
--       accessible again with SELECT COUNT(*).


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Time Travel Queries
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Delete all employees from emp_copy where deptno = 10, then:
--       A) Capture the DELETE statement's query ID with SET del_qid.
--       B) Query emp_copy BEFORE(STATEMENT => $del_qid) to confirm the
--          deleted rows are still visible through Time Travel.
--       C) Query emp_copy AT(STATEMENT => $del_qid) and confirm deptno=10
--          rows are gone (the delete is included in AT).


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Restore Deleted Rows
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using the $del_qid from Exercise 3:
--       A) Create a table called emp_dept10_restored that contains only the
--          rows that were deleted (deptno = 10) by selecting them from
--          emp_copy BEFORE(STATEMENT => $del_qid) with a WHERE deptno = 10.
--       B) Verify the restored table has the correct rows with SELECT *.
--       C) Re-insert the restored rows back into emp_copy using
--          INSERT INTO emp_copy SELECT * FROM emp_dept10_restored.
--       D) Confirm emp_copy now has the same total row count as demo_db.scott.emp.


-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- Task C – YOUR CODE HERE


-- Task D – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop objects created in this exercise set only.
-- Do NOT drop demo_db — used in all remaining labs.

DROP TABLE IF EXISTS demo_db.demo_schema.emp_copy;
DROP TABLE IF EXISTS demo_db.demo_schema.emp_dept10_restored;
