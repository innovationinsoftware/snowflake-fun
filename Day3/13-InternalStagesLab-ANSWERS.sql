/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Internal stage types — user stage (@~), table stage (@%table), named stage
2) LIST / LS — inspecting stage contents
3) PUT command — uploading local files via SnowSQL
4) Querying staged files with positional columns and metadata columns
5) FILE FORMAT objects for stage queries
6) PATTERN and PATH filters on staged data
7) RM / REMOVE — deleting files from a stage
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Stage Types and LIST
-- ──────────────────────────────────────────────────────────────────────────────

USE DATABASE movies_db;
USE SCHEMA movies_schema;

CREATE OR REPLACE TABLE reviews
(
    id       INT,
    reviewer STRING,
    score    FLOAT,
    notes    STRING
);

-- Task A: User stage
LIST @~;

-- Task B: Table stage for reviews
LS @%reviews;

-- Task C: Named stage
LIST @movies_stage;

-- [TEACHING NOTE]
-- @~ is always present and belongs only to the current user — other users
-- cannot see or access it even with full account privileges.
-- @%reviews is automatically created when the reviews table is created;
-- it is tied to the table's lifetime and dropped when the table is dropped.
-- @movies_stage is a named stage — it survives independent of any table and
-- can be granted to roles for shared use across teams.
-- Common mistake: trying LIST @%reviews before the reviews table exists —
-- the table stage only materialises after the CREATE TABLE statement executes.
-- Discussion point: "Which stage type would you use for a shared ETL pipeline
-- that multiple roles need to access?"
-- (Answer: a named stage — it can be granted independently of table ownership.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ FILE FORMAT Comparison
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FILE FORMAT csv_no_header
    TYPE        = CSV
    SKIP_HEADER = 0;

SELECT
    metadata$filename AS file_name,
    $1,
    $2,
    $3
FROM @movies_stage (FILE_FORMAT => 'csv_no_header', PATTERN => '.*[.]csv') t;

-- Answer: The first row with csv_no_header returns the header row as data
-- (e.g. $1 = 'id', $2 = 'title', $3 = 'release_date') because SKIP_HEADER = 0
-- does not skip any rows. The Demo 5b query with csv_file_format (SKIP_HEADER = 1)
-- skips the first row and returns only data rows.

-- [TEACHING NOTE]
-- SKIP_HEADER = 0 is the default — it must be explicitly set to 1 for CSV files
-- that include a column header row, or the header values appear as data.
-- In production, incorrect SKIP_HEADER causes silent data quality issues:
-- the header row is loaded as a record with string values in numeric columns,
-- which either fails type conversion or inserts NULL/0 into numeric fields.
-- Common mistake: assuming the file format is validated at CREATE time —
-- Snowflake only applies the format at query/load time, so errors surface there.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ RESULT_SCAN on LIST
-- ──────────────────────────────────────────────────────────────────────────────

LIST @movies_stage;

SET lst_qid = (SELECT LAST_QUERY_ID());

SELECT
    "name",
    "size"
FROM TABLE(RESULT_SCAN($lst_qid));

-- [TEACHING NOTE]
-- LIST returns a fixed schema with columns: name, size, md5, last_modified.
-- Column names from SHOW and LIST commands are always lowercase double-quoted
-- strings in RESULT_SCAN — "name" not NAME.
-- The number of rows equals the number of files in the stage. If movies.csv
-- was PUT in Demo 3, there should be 1 file. If PUT was not run, 0 rows appear.
-- Common mistake: referencing the column as "NAME" (uppercase) instead of "name"
-- (lowercase) — Snowflake identifier matching in double quotes is case-sensitive.
-- Early finishers: chain LIST and RESULT_SCAN using the ->> operator:
--   LIST @movies_stage ->> SELECT "name", "size" FROM $1;


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Stage Cleanup and Verification
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: Remove all files from the named stage
RM @movies_stage;

-- Task B: Verify the stage is empty
LIST @movies_stage;

SET lst_empty_qid = (SELECT LAST_QUERY_ID());

SELECT COUNT(*) AS file_count
FROM TABLE(RESULT_SCAN($lst_empty_qid));

-- Task C: Drop the csv_no_header file format
DROP FILE FORMAT IF EXISTS movies_db.movies_schema.csv_no_header;

-- [TEACHING NOTE]
-- Task A: RM with no path filter removes all files from the stage. Specifying
-- a filename (RM @movies_stage/movies.csv) removes only that file — important
-- when multiple files share a stage.
-- Task B: COUNT(*) = 0 confirms the stage is empty. An empty LIST does not
-- raise an error — it simply returns zero rows in RESULT_SCAN.
-- Task C: FILE FORMAT objects must be explicitly dropped — they are not
-- automatically cleaned up when the stage or table is dropped.
-- Common mistake in Task A: using DROP STAGE instead of RM — DROP STAGE
-- removes the stage object itself (the metadata pointer), while RM removes
-- only the files inside it. After RM, the stage still exists but is empty.
-- Discussion point: "When would you use RM vs DROP STAGE?"
-- (Answer: RM clears files while keeping the stage for future loads;
--  DROP STAGE removes the stage definition permanently.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

-- DROP DATABASE IF EXISTS movies_db;
DROP TABLE       IF EXISTS movies_db.movies_schema.reviews;
DROP FILE FORMAT IF EXISTS movies_db.movies_schema.csv_no_header;
