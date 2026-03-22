/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Zero-copy cloning — tables, schemas, and databases
2) Clone-of-a-clone behaviour
3) Database-level cloning and recursive object copying
4) Data independence after cloning
5) Point-in-time cloning using Time Travel OFFSET
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
-- demo_db.scott is the canonical schema used throughout Day 2 labs.
-- demo_db.demo_schema is created fresh here for objects owned by this lab.
-- Pre-requisite: SCHEMA-SETUP-SCOTT.sql must have been executed so that
-- demo_db.scott.emp and demo_db.scott.dept exist.

USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS demo_db;
USE DATABASE demo_db;

CREATE SCHEMA IF NOT EXISTS demo_schema;
USE SCHEMA demo_schema;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Zero-Copy Table Cloning
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- CREATE TABLE ... CLONE is a metadata-only operation — no data is physically
-- copied. Both the source and clone share the same underlying micro-partitions
-- until one of them is modified (copy-on-write).
-- A clone of a clone works identically: emp_clone_two shares storage with
-- emp_clone which shares storage with scott.emp.

CREATE OR REPLACE TABLE emp_clone CLONE scott.emp;

SELECT * FROM emp_clone;

-- 2b. Clone of a clone
CREATE OR REPLACE TABLE emp_clone_two CLONE emp_clone;

SELECT * FROM emp_clone_two;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Database-Level Cloning
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Cloning a database is recursive: all schemas, tables, views, and stages
-- inside it are cloned in a single metadata operation regardless of size.
-- SHOW TABLES inside the clone confirms the full schema was reproduced.
-- The clone is immediately fully independent — DDL and DML on either side
-- does not affect the other.

CREATE OR REPLACE DATABASE demo_db_clone CLONE demo_db;

USE DATABASE demo_db_clone;
USE SCHEMA scott;

SHOW TABLES;

SELECT * FROM dept;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Data Independence After Cloning
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Inserting a row into the cloned table triggers copy-on-write for the affected
-- micro-partitions — new storage is allocated only for the changed data.
-- The source table in demo_db.scott remains unchanged, proving independence.
-- Wait 2–3 minutes before inserting so the Time Travel demo in Demo 5 is
-- meaningful (the offset must cross the clone creation boundary).

-- 4a. Modify the clone — insert a new department
INSERT INTO dept(deptno, dname, loc) VALUES (50, 'HR', 'MIAMI');

-- 4b. Clone shows the new row
SELECT * FROM dept;

-- 4c. Source is unchanged
SELECT * FROM "DEMO_DB"."SCOTT"."DEPT";


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Point-in-Time Cloning with Time Travel
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- CLONE ... AT(OFFSET => -N) creates a clone of the table as it existed
-- N seconds in the past. This is the primary mechanism for creating
-- "snapshot" backups without any storage cost until data diverges.
-- The offset must be large enough to pre-date the INSERT in Demo 4.

SET offset_min = 1;

-- Wait for about 1 min before running the next statement.

SELECT *
FROM dept AT(OFFSET => -60 * $offset_min);

CREATE OR REPLACE TABLE dept_clone_time_travel CLONE dept
    AT(OFFSET => -60 * $offset_min);

SELECT * FROM dept_clone_time_travel;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- demo_db_clone was created only for this demo. Drop it after the demo.
-- demo_db must NOT be dropped — it is used by every remaining Day 2 lab
-- and by Day 4 labs (Streams, Tasks, Cortex).

DROP DATABASE IF EXISTS demo_db_clone;
-- DROP DATABASE IF EXISTS demo_db;   -- keep: used in all remaining labs


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create objects inside demo_db.demo_schema.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Zero-Copy Table Clone
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using demo_db.demo_schema as your context, create a table called
--       dept_clone as a clone of demo_db.scott.dept.
--       Verify the clone contains the same rows as the source by querying
--       both tables and comparing the results with COUNT(*).

USE DATABASE demo_db;
USE SCHEMA demo_schema;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Data Independence
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Insert a new row into your dept_clone table:
--         deptno = 60, dname = 'ANALYTICS', loc = 'AUSTIN'
--       Then query both dept_clone and demo_db.scott.dept to confirm
--       the source table was NOT affected by the insert.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Schema-Level Cloning
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a clone of the entire demo_db.scott schema called scott_backup
--       inside demo_db.  After cloning, run SHOW TABLES IN SCHEMA scott_backup
--       to confirm the same tables exist as in demo_db.scott.
--       (Hint: CREATE SCHEMA scott_backup CLONE demo_db.scott;)


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Point-in-Time Clone
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Wait at least 1 minute after completing Exercise 2, then:
--       A) Query dept_clone as it existed 1 minute ago using AT(OFFSET => -60).
--          The row you inserted in Exercise 2 (deptno=60) should NOT appear.
--       B) Create a table called dept_before_insert as a clone of dept_clone
--          AT(OFFSET => -60) to capture the pre-insert state permanently.
--       C) Confirm dept_before_insert does NOT contain deptno = 60.


-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- Task C – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop only the objects created in this exercise set.
-- Do NOT drop demo_db — it is used in all remaining Day 2 and Day 4 labs.

DROP TABLE IF EXISTS demo_db.demo_schema.dept_clone;
DROP TABLE IF EXISTS demo_db.demo_schema.dept_before_insert;
DROP SCHEMA IF EXISTS demo_db.scott_backup;
