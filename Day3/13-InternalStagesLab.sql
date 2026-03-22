/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
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
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A dedicated database and schema are created for this lab to keep stage objects
-- isolated. The movies table is created now so its table stage (@%movies) exists
-- and can be listed in Demo 2.

USE ROLE sysadmin;

CREATE DATABASE IF NOT EXISTS movies_db;
USE DATABASE movies_db;

CREATE SCHEMA IF NOT EXISTS movies_schema;
USE SCHEMA movies_schema;

CREATE OR REPLACE TABLE movies
(
    id           INT,
    title        STRING,
    release_date DATE
);


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Internal Stage Types and LIST
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Snowflake provides three types of internal stages:
--   User stage  (@~)          — private to the current user, always exists
--   Table stage (@%table)     — tied to a specific table, always exists
--   Named stage (@stage_name) — explicit object, must be created, shareable
-- LIST and its alias LS are metadata operations — no warehouse required.
-- The user stage may already contain files from previous worksheet uploads.

-- 2a. User stage
LIST @~;

-- 2b. Table stage
LS @%movies;

-- 2c. Create and list a named stage
CREATE OR REPLACE STAGE movies_stage;

LS @movies_stage;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ PUT — Uploading a Local File via SnowSQL
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- PUT uploads a local file to a Snowflake internal stage. It runs exclusively
-- inside SnowSQL (the CLI client) — it cannot be executed from the web UI.
-- AUTO_COMPRESS = FALSE preserves the original file without gzip compression,
-- which makes the uploaded file easier to inspect and query directly.
-- The path must not contain spaces; use the movies.csv provided in the Day3 folder.

USE ROLE sysadmin;
USE DATABASE movies_db;
USE SCHEMA movies_schema;

-- Run these three PUT commands from SnowSQL:
PUT file://C:\Personal\Training\movies.csv @~             AUTO_COMPRESS = FALSE;
PUT file://C:\Personal\Training\movies.csv @%movies       AUTO_COMPRESS = FALSE;
PUT file://C:\Personal\Training\movies.csv @movies_stage  AUTO_COMPRESS = FALSE;

LS @~/movies.csv;
LS @%movies;
LS @movies_stage;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Querying Staged Files — Positional Columns
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Files in a stage can be queried directly using $1, $2, $3 positional column
-- references before loading into a table. This allows data preview and validation
-- without consuming any load quota. The query requires a running warehouse.

SELECT $1, $2, $3
FROM @~/movies.csv;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ FILE FORMAT and Metadata Columns
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A named FILE FORMAT object instructs Snowflake how to parse the file:
-- delimiter, header rows, encoding, null handling, etc.
-- metadata$filename and metadata$file_row_number are always available when
-- querying a stage — useful for auditing, debugging, and tracing row origins.
-- PATTERN filters by filename regex; a path prefix (e.g. @~/movies.csv) limits
-- the query to a specific file by path.

CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE        = CSV
    SKIP_HEADER = 1;

-- 5a. Table stage with file format — metadata columns visible
SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    $1                       AS id,
    $2                       AS title,
    $3                       AS release_date
FROM @%movies (FILE_FORMAT => 'csv_file_format');

-- 5b. Named stage with PATTERN filter
SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    $1                       AS id,
    $2                       AS title,
    $3                       AS release_date
FROM @movies_stage (FILE_FORMAT => 'csv_file_format', PATTERN => '.*[.]csv') t;

-- 5c. User stage with exact PATH filter
SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    $1                       AS id,
    $2                       AS title,
    $3                       AS release_date
FROM @~/movies.csv (FILE_FORMAT => 'csv_file_format') t;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ RM — Removing Files from a Stage
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- RM (alias REMOVE) permanently deletes files from an internal stage.
-- There is no recycle bin — once removed, the file must be re-uploaded with PUT.
-- Removing from a table stage clears only the staged files, not the table data.

RM @~/movies.csv;
RM @%movies;
RM @movies_stage;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- movies_db is not referenced by any later lab and can be dropped after the demo.

-- DROP DATABASE IF EXISTS movies_db;   -- safe to drop after this lab


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create objects in movies_db.movies_schema.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Stage Types and LIST
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a table called reviews in movies_db.movies_schema with columns:
--         id INT, reviewer STRING, score FLOAT, notes STRING
--       Then confirm three stage types are accessible by running:
--         A) LIST @~ — user stage
--         B) LS @%reviews — table stage for the reviews table
--         C) LS @movies_stage — named stage

USE DATABASE movies_db;
USE SCHEMA movies_schema;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ FILE FORMAT Comparison
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a CSV file format called csv_no_header with TYPE = CSV and
--       SKIP_HEADER = 0.
--       Query @movies_stage using this format and return:
--         metadata$filename AS file_name, $1, $2, $3
--       How does the first row of output differ from the Demo 5b query
--       that used csv_file_format (SKIP_HEADER = 1)?
--       Answer in a comment below your query.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ RESULT_SCAN on LIST
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Run LIST @movies_stage, capture the query ID with SET, then use
--       RESULT_SCAN to display only the "name" and "size" columns.
--       How many files are currently staged?

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Stage Cleanup and Verification
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: A) Remove all files from @movies_stage using RM.
--       B) Run LIST @movies_stage and use RESULT_SCAN to confirm the stage
--          is now empty (zero rows returned).
--       C) Drop the csv_no_header file format created in Exercise 2.
--       Hint: LIST on an empty stage returns zero rows, not an error.

-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- Task C – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- movies_db is not referenced by any later lab and is safe to drop entirely.

-- DROP DATABASE IF EXISTS movies_db;   -- uncomment when finished with this lab
DROP TABLE       IF EXISTS movies_db.movies_schema.reviews;
DROP FILE FORMAT IF EXISTS movies_db.movies_schema.csv_no_header;
