/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) DATA_RETENTION_TIME_IN_DAYS — account, database, schema, and table levels
2) Retention time inheritance and override hierarchy
3) UNDROP — recovering dropped tables
4) SHOW TABLES HISTORY — visibility of dropped objects
5) Time Travel queries — AT(OFFSET), AT(STATEMENT), AT(TIMESTAMP), BEFORE(STATEMENT)
6) Restoring data from Time Travel into a new table
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Retention Time Settings
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Clone emp, check/set retention to 3 days, confirm via RESULT_SCAN.

USE DATABASE demo_db;
USE SCHEMA demo_schema;

CREATE OR REPLACE TABLE emp_copy CLONE demo_db.scott.emp;

-- A) Check current retention_time
SHOW TABLES LIKE 'emp_copy';

SELECT
    "name",
    "retention_time"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- B) Set retention to 3 days
ALTER TABLE emp_copy SET DATA_RETENTION_TIME_IN_DAYS = 3;

-- C) Confirm
SHOW TABLES LIKE 'emp_copy';

SELECT
    "name",
    "retention_time"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- [TEACHING NOTE]
-- The initial retention_time is inherited from the schema (or database, or account).
-- If demo_schema was set to 0 in Lab 06's Demo 2f, the initial value will be 0 —
-- the ALTER TABLE override restores per-table retention independently.
-- Discussion point: why might you want table-level retention different from
-- the schema default?
-- (Answer: high-churn staging tables don't need 90-day retention — set them to
--  1 or 0 to reduce Time Travel storage costs; critical dimension tables may
--  warrant maximum retention for auditing.)
-- Common mistake: running SHOW TABLES without the LIKE filter and scanning all
-- tables to find emp_copy — LIKE reduces the result set and is a production habit.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ UNDROP
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Drop emp_copy, confirm in SHOW TABLES HISTORY, then UNDROP and verify.

DROP TABLE emp_copy;

SHOW TABLES HISTORY;

SELECT
    "name",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'EMP_COPY';

UNDROP TABLE emp_copy;

SELECT COUNT(*) AS row_count FROM emp_copy;

-- [TEACHING NOTE]
-- After DROP, dropped_on is populated (not NULL) in SHOW TABLES HISTORY.
-- After UNDROP, dropped_on returns to NULL and the table is fully accessible.
-- UNDROP restores the most recent version — if the same table was dropped twice,
-- two UNDROP calls are needed to restore both versions (each UNDROP pops one).
-- Common mistake: forgetting that UNDROP fails if a table with the same name
-- already exists — if you recreated emp_copy after dropping it, UNDROP the
-- recreated version first, then retry.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Time Travel Queries
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Delete deptno=10 rows, capture query ID, then query AT and BEFORE.

DELETE FROM emp_copy WHERE deptno = 10;

SET del_qid = (SELECT LAST_QUERY_ID());

-- B) BEFORE(STATEMENT) — deleted rows are still visible
SELECT *
FROM emp_copy
BEFORE(STATEMENT => $del_qid)
WHERE deptno = 10;

-- C) AT(STATEMENT) — deleted rows are gone (the delete is included)
SELECT *
FROM emp_copy
AT(STATEMENT => $del_qid)
WHERE deptno = 10;

-- [TEACHING NOTE]
-- BEFORE(STATEMENT) returns the table as it was immediately before the DELETE —
-- the deptno=10 rows are present.
-- AT(STATEMENT) returns the table as it was after the DELETE committed —
-- the deptno=10 rows are absent (zero rows returned with WHERE deptno=10).
-- The distinction between AT and BEFORE is subtle but critical for precise
-- point-in-time recovery.
-- Common mistake: running AT and BEFORE in the wrong order or forgetting to
-- SET del_qid immediately after the DELETE — any intervening statement changes
-- LAST_QUERY_ID() and the wrong query ID is captured.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Restore Deleted Rows
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: Create emp_dept10_restored from the deleted rows
CREATE TABLE emp_dept10_restored AS
SELECT *
FROM emp_copy
BEFORE(STATEMENT => $del_qid)
WHERE deptno = 10;

-- Task B: Verify
SELECT * FROM emp_dept10_restored;

-- Task C: Re-insert into emp_copy
INSERT INTO emp_copy
SELECT * FROM emp_dept10_restored;

-- Task D: Confirm total count matches source
SELECT COUNT(*) AS emp_copy_count  FROM emp_copy;
SELECT COUNT(*) AS source_count    FROM demo_db.scott.emp;

-- [TEACHING NOTE]
-- The CTAS + INSERT pattern is the standard surgical row restore: it recovers
-- only specific rows rather than replacing the entire table.
-- An alternative (quicker for full-table restore): CREATE OR REPLACE TABLE
-- emp_copy AS SELECT * FROM emp_copy BEFORE(STATEMENT => $del_qid) — this
-- replaces the entire table content in one statement.
-- Common mistake in Task C: INSERT ... SELECT without matching column order —
-- if emp_copy was created with a different column order than emp_dept10_restored,
-- the insert may succeed but with values in the wrong columns. Always verify
-- with DESC TABLE on both sides first.
-- Early finishers: add a WHERE clause to the Task B SELECT to confirm the
-- restored table has exactly the same empno values as the original deptno=10 rows
-- in demo_db.scott.emp.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS demo_db.demo_schema.emp_copy;
DROP TABLE IF EXISTS demo_db.demo_schema.emp_dept10_restored;
