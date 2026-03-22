/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Zero-copy cloning — tables, schemas, and databases
2) Clone-of-a-clone behaviour
3) Database-level cloning and recursive object copying
4) Data independence after cloning
5) Point-in-time cloning using Time Travel OFFSET
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Zero-Copy Table Clone
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create dept_clone as a clone of demo_db.scott.dept, verify with COUNT(*).

USE DATABASE demo_db;
USE SCHEMA demo_schema;

CREATE TABLE dept_clone CLONE demo_db.scott.dept;

SELECT COUNT(*) AS clone_row_count FROM dept_clone;
SELECT COUNT(*) AS source_row_count FROM demo_db.scott.dept;

-- [TEACHING NOTE]
-- Both counts must match — the clone is a perfect copy at the metadata level.
-- No data was physically duplicated; both tables share the same micro-partitions.
-- Discussion point: at what point does storage cost for the clone begin to accrue?
-- (Answer: when either the source or the clone is modified — copy-on-write
--  allocates new micro-partitions only for the changed data.)
-- Common mistake: cloning without USE SCHEMA first and landing the object in
-- the wrong schema — always confirm context with SELECT CURRENT_SCHEMA().


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Data Independence
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Insert deptno=60 into dept_clone, verify source is unchanged.

INSERT INTO dept_clone(deptno, dname, loc) VALUES (60, 'ANALYTICS', 'AUSTIN');

SELECT * FROM dept_clone ORDER BY deptno;

SELECT * FROM demo_db.scott.dept ORDER BY deptno;

-- [TEACHING NOTE]
-- dept_clone now has 5 rows; demo_db.scott.dept still has 4.
-- The INSERT triggered copy-on-write on the affected micro-partitions in
-- dept_clone only — the source micro-partitions are untouched.
-- Common mistake: querying both tables without ORDER BY and assuming row order
-- proves independence — always ORDER BY a key to make the comparison meaningful.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Schema-Level Cloning
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Clone demo_db.scott schema to scott_backup, verify with SHOW TABLES.

CREATE SCHEMA demo_db.scott_backup CLONE demo_db.scott;

SHOW TABLES IN SCHEMA demo_db.scott_backup;

SHOW TABLES IN SCHEMA demo_db.scott;

-- [TEACHING NOTE]
-- Both SHOW TABLES outputs must list the same table names — the clone is recursive.
-- Schema cloning copies tables, views, sequences, and stages but NOT grants —
-- privileges must be re-applied separately on the clone if needed.
-- Discussion point: what is NOT cloned when you clone a schema?
-- (Answer: role grants, resource monitors, tasks, streams — these are
--  account-level or change-tracking objects not included in a schema clone.)
-- Common mistake: trying CREATE SCHEMA ... CLONE with a fully qualified source
-- but without specifying the target database — the schema lands in the current
-- database by default. Always double-check with SHOW SCHEMAS IN DATABASE demo_db.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Point-in-Time Clone
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: Query dept_clone as it existed 1 minute ago
SELECT *
FROM demo_db.demo_schema.dept_clone
AT(OFFSET => -1*60);

-- Task B: Capture pre-insert state as a permanent table
CREATE TABLE demo_db.demo_schema.dept_before_insert CLONE demo_db.demo_schema.dept_clone
AT(OFFSET => -1*60);

-- Task C: Confirm deptno=60 is NOT in the pre-insert snapshot
SELECT * FROM demo_db.demo_schema.dept_before_insert ORDER BY deptno;

-- [TEACHING NOTE]
-- Task A should return 4 rows (no deptno=60) if run > 1 minute after Exercise 2.
-- If the offset is too small, Snowflake returns an error: "time travel data is
-- not available for the requested time" — the fix is to wait and retry.
-- Task B materialises the Time Travel snapshot as a permanent table, costing
-- no additional storage until it diverges from dept_clone's pre-insert state.
-- Common mistake in Task C: forgetting the ORDER BY and assuming NULL means
-- deptno=60 is absent — always use a WHERE deptno = 60 to be explicit:
--   SELECT COUNT(*) FROM dept_before_insert WHERE deptno = 60;  → must be 0
-- Early finishers: try cloning dept_clone AT(STATEMENT => <insert_qid>) to
-- capture the state at the exact moment of the INSERT using a query ID instead
-- of a time offset.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS demo_db.demo_schema.dept_clone;
DROP TABLE IF EXISTS demo_db.demo_schema.dept_before_insert;
DROP SCHEMA IF EXISTS demo_db.scott_backup;
