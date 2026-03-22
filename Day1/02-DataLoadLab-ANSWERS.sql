/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Creating databases, schemas, and tables
2) External stages (S3)
3) CSV file formats and format options
4) COPY INTO — bulk loading CSV data
5) ON_ERROR and PATTERN attributes
6) VALIDATE function and load error inspection
7) Idempotency — Snowflake load history and TRUNCATE
8) NULL_IF — fixing empty-field data quality issues
9) JSON data — VARIANT column, stage, load, and semi-structured querying
10) Creating views over JSON data and cross-database joins
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Stage Exploration
-- ──────────────────────────────────────────────────────────────────────────────
-- Task A: LIST citibike_trips, filter RESULT_SCAN to .json files only.
-- Task B: Explain why mixed file types break a plain COPY.

USE DATABASE citibike;
USE SCHEMA public;

LIST @citibike_trips;

SET id = (SELECT LAST_QUERY_ID());

SELECT *
FROM TABLE(RESULT_SCAN($id))
WHERE "name" LIKE '%.json';

-- Task B answer:
-- There are several JSON files in the stage. A plain COPY INTO trips using the
-- csv file format fails because Snowflake attempts to parse every file it finds,
-- and the JSON files do not conform to the CSV structure — causing a parse error
-- or a column count mismatch on those files.

-- [TEACHING NOTE]
-- This exercise builds observational skill before loading — an important habit.
-- The LIKE '%.json' filter on "name" from RESULT_SCAN is a direct extension of
-- what students saw in Demo 2 with LIKE '%citibike%'.
-- Discussion point: what other strategies exist besides PATTERN to control which
-- files are loaded?
-- (Answer: FILES = ('file1.csv.gz', 'file2.csv.gz') for explicit file lists;
--  or organise the stage into subfolders and use @stage/subfolder/ as the source.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ File Format Options
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create csv_strict with ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
--       and NULL_IF = (''), then DESC it.

CREATE OR REPLACE FILE FORMAT citibike.public.csv_strict
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    EMPTY_FIELD_AS_NULL            = TRUE
    SKIP_HEADER                    = 1
    NULL_IF                        = ('');

DESC FILE FORMAT citibike.public.csv_strict;

-- Two options that differ from the demo csv format:
--   1. ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE  (demo had FALSE)
--   2. NULL_IF = ('')                         (demo had this only in the fixed version)

-- [TEACHING NOTE]
-- This exercise reinforces that file formats are named, reusable objects —
-- not inline settings. Having a stricter variant (csv_strict) alongside a
-- permissive one (csv) is a realistic production pattern: use strict for
-- validated, trusted sources and permissive for exploratory or legacy data.
-- Common mistake: students create the format without a three-part name,
-- meaning it lands in whatever schema their current context points to.
-- Always qualify: DATABASE.SCHEMA.FORMAT_NAME.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Selective Load with PATTERN and ON_ERROR
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create trips_copy, load from @citibike_trips with PATTERN + ON_ERROR,
--       then VALIDATE and COUNT.

CREATE OR REPLACE TABLE citibike.public.trips_copy
(
    tripduration              INTEGER,
    starttime                 TIMESTAMP,
    stoptime                  TIMESTAMP,
    start_station_id          INTEGER,
    start_station_name        STRING,
    start_station_latitude    FLOAT,
    start_station_longitude   FLOAT,
    end_station_id            INTEGER,
    end_station_name          STRING,
    end_station_latitude      FLOAT,
    end_station_longitude     FLOAT,
    bikeid                    INTEGER,
    membership_type           STRING,
    usertype                  STRING,
    birth_year                INTEGER,
    gender                    INTEGER
);

COPY INTO citibike.public.trips_copy
FROM @citibike.public.citibike_trips
FILE_FORMAT = citibike.public.csv_strict
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';

SET id = (SELECT LAST_QUERY_ID());

-- Task A: validate
SELECT *
FROM TABLE(VALIDATE(citibike.public.trips_copy, JOB_ID => $id));

-- Task B: confirm row count
SELECT COUNT(*) AS row_count
FROM citibike.public.trips_copy;

-- [TEACHING NOTE]
-- Because csv_strict has ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE, any row where
-- the number of delimited fields does not match the table's column count will
-- be rejected and appear in VALIDATE output rather than silently truncating.
-- Students will likely see more errors here than in the demo (which used FALSE).
-- This is intentional — it shows the trade-off between strict and permissive formats.
-- Discussion point: which format would you choose for a production pipeline? Why?
-- (Answer: strict in production catches data quality issues early; permissive
--  is acceptable for exploratory loads where row count matters more than purity.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Idempotency and JSON Querying
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: re-run the COPY — row count must not change
COPY INTO citibike.public.trips_copy
FROM @citibike.public.citibike_trips
FILE_FORMAT = citibike.public.csv_strict
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';

SELECT COUNT(*) AS row_count
FROM citibike.public.trips_copy;

-- Answer: row count is unchanged because Snowflake's load history records every
-- file already loaded into trips_copy. The second COPY sees all files as "LOADED"
-- and skips them — 0 new rows are inserted.

-- Task B: query json_weather_data directly using three-part name
SELECT
    v:city.name::STRING        AS city_name,
    v:main.temp::FLOAT - 273.15 AS temp_celsius
FROM weather.public.json_weather_data
WHERE v:city.id::INT = 5128638
ORDER BY temp_celsius ASC
LIMIT 5;

-- [TEACHING NOTE]
-- Task A reinforces idempotency from Demo 6 — but now students observe it on
-- their own table rather than the instructor's, making the lesson personal.
-- Task B uses the three-part name weather.public.json_weather_data so no
-- USE DATABASE is required. This is the correct production habit: explicit
-- qualification avoids context-dependent failures when scripts run in
-- automated pipelines where the session context may differ.
-- Common mistake in Task B: casting v:main.temp directly without parentheses
-- before subtracting 273.15, which causes an operator precedence error.
-- The safe form is always: (v:main.temp::FLOAT) - 273.15


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- trips_copy and csv_strict were created only for this exercise set.
-- Drop them when finished.  Do NOT drop the citibike or weather databases —
-- they are used in later labs.

DROP TABLE IF EXISTS citibike.public.trips_copy;
DROP FILE FORMAT IF EXISTS citibike.public.csv_strict;
