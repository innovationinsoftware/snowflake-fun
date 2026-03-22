/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
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
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Database, Schema, and Table Setup
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- All objects in Snowflake live inside a three-level hierarchy:
--   DATABASE → SCHEMA → OBJECT (table, view, stage, file format, …)
-- SYSADMIN is the correct role for creating data objects; ACCOUNTADMIN is
-- reserved for account-level administration.
-- CREATE DATABASE also creates a default PUBLIC schema automatically.

USE ROLE sysadmin;

CREATE DATABASE IF NOT EXISTS citibike;

USE DATABASE citibike;
USE SCHEMA public;

CREATE OR REPLACE TABLE trips
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


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ External Stage — Pointing at S3
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A STAGE is a named pointer to a file location — internal (Snowflake-managed)
-- or external (S3, Azure Blob, GCS). No data is copied at this point; the
-- stage is just a reference. LIST @stage shows what files are available.
-- RESULT_SCAN($id) turns the LIST output into a queryable result set — the
-- same pattern used with SHOW commands.

CREATE OR REPLACE STAGE citibike_trips
    URL = 's3://snowflake-workshop-lab/japan/citibike-trips';

LIST @citibike_trips;

SET id = (SELECT LAST_QUERY_ID());

SELECT *
FROM TABLE(RESULT_SCAN($id))
WHERE "name" LIKE '%citibike%'
LIMIT 10;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ CSV File Format
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A FILE FORMAT is a named, reusable object that defines how Snowflake parses
-- files. Key options shown here:
--   FIELD_OPTIONALLY_ENCLOSED_BY — handles quoted fields that contain commas
--   ERROR_ON_COLUMN_COUNT_MISMATCH — tolerates ragged rows (useful for CSVs)
--   EMPTY_FIELD_AS_NULL — treats empty fields as NULL rather than empty string
--   SKIP_HEADER = 1 — skips the column header row

CREATE OR REPLACE FILE FORMAT csv
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL            = TRUE
    SKIP_HEADER                    = 1;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ COPY INTO — Intentional Failure (Mixed File Types)
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- This COPY deliberately fails because the stage contains both .csv.gz and
-- .json files. Without a PATTERN filter, Snowflake tries to parse every file
-- using the CSV format — and the JSON files cause an error.
-- The lesson: always inspect stage contents with LIST before loading.

COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ COPY INTO — PATTERN, ON_ERROR, and VALIDATE
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Three important COPY INTO attributes:
--   PATTERN — a regex applied to file names; only matching files are loaded
--   ON_ERROR = CONTINUE — skips bad rows and continues loading the rest
--   VALIDATE() — returns the errors from the most recent COPY job by JOB_ID
-- Discussion point: "Why use ON_ERROR = CONTINUE instead of ABORT_STATEMENT?"
-- (Answer: ABORT_STATEMENT rolls back the entire load on any row error.
--  CONTINUE maximises the number of rows loaded when source data is imperfect.)

-- 5a. Load only .csv.gz files, skipping any row-level errors
COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';

SET id = (SELECT LAST_QUERY_ID());

-- 5b. Capture errors into a persistent table for analysis
CREATE OR REPLACE TABLE trips_load_errors AS
SELECT *
FROM TABLE(VALIDATE(trips, JOB_ID => $id));

-- 5c. Inspect the errors
SELECT *
FROM trips_load_errors
LIMIT 10;

-- 5d. Confirm data was loaded despite errors
SELECT *
FROM trips
LIMIT 10;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ Idempotency — Snowflake Load History
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Snowflake tracks every file loaded into a table via internal load metadata.
-- Re-running the same COPY statement does not reload already-loaded files —
-- the output shows 0 rows copied and status "LOADED" for each file.
-- This is idempotency: the same command can be run multiple times safely.
-- To force a reload, the table must be TRUNCATE'd first (which also clears
-- the load history for that table).

COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 7 │ NULL_IF — Fixing Empty-Field Data Quality Issues
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- EMPTY_FIELD_AS_NULL handles truly empty CSV fields (,,).
-- NULL_IF = ('') additionally converts the literal empty string '' to NULL.
-- Without NULL_IF, '' is stored as an empty string rather than NULL, causing
-- type-cast failures for INTEGER and FLOAT columns.
-- TRUNCATE TABLE also resets the file load history, allowing a clean reload.

-- 7a. Add NULL_IF to the file format
CREATE OR REPLACE FILE FORMAT csv
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL            = TRUE
    SKIP_HEADER                    = 1
    NULL_IF                        = ('');

-- 7b. Reset the table and its load history, then reload
TRUNCATE TABLE trips;

COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';

SET id = (SELECT LAST_QUERY_ID());

-- 7c. Confirm zero errors after the NULL_IF fix
SELECT *
FROM TABLE(VALIDATE(trips, JOB_ID => $id));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 8 │ JSON Data — VARIANT Column, Stage, Load, View, and Cross-DB Join
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Snowflake stores semi-structured data (JSON, Avro, Parquet, XML) in a
-- VARIANT column — a flexible, self-describing binary format. No schema is
-- needed at load time; columns are extracted at query time using colon notation:
--   v:city.name::STRING  → dot path + explicit CAST to a SQL type
--   v:weather[0].main    → array indexing with [n]
-- The view flattens the JSON into typed relational columns.
-- The final join crosses two databases (citibike and weather) using
-- three-part names — possible because both databases live in the same account.

-- 8a. Set up the weather database
USE ROLE accountadmin;
DROP DATABASE IF EXISTS weather;
GRANT ALL ON WAREHOUSE compute_wh TO ROLE sysadmin;

USE ROLE sysadmin;
USE WAREHOUSE compute_wh;

CREATE DATABASE IF NOT EXISTS weather;
USE DATABASE weather;
USE SCHEMA public;

-- 8b. Create a VARIANT table and an external stage pointing at JSON files
CREATE TABLE json_weather_data (v VARIANT);

CREATE STAGE nyc_weather
    URL = 's3://snowflake-workshop-lab/weather-nyc';

LIST @nyc_weather;

-- 8c. Load the JSON files — no FILE FORMAT needed; TYPE = json is inline
COPY INTO json_weather_data
FROM @nyc_weather
FILE_FORMAT = (TYPE = json);

SELECT *
FROM json_weather_data
LIMIT 10;

-- 8d. Create a view that flattens JSON paths to typed relational columns
CREATE VIEW json_weather_data_view AS
SELECT
    v:time::TIMESTAMP                  AS observation_time,
    v:city.id::INT                     AS city_id,
    v:city.name::STRING                AS city_name,
    v:city.country::STRING             AS country,
    v:city.coord.lat::FLOAT            AS city_lat,
    v:city.coord.lon::FLOAT            AS city_lon,
    v:clouds.all::INT                  AS clouds,
    (v:main.temp::FLOAT) - 273.15      AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15  AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15  AS temp_max,
    v:weather[0].main::STRING          AS weather,
    v:weather[0].description::STRING   AS weather_desc,
    v:weather[0].icon::STRING          AS weather_icon,
    v:wind.deg::FLOAT                  AS wind_dir,
    v:wind.speed::FLOAT                AS wind_speed
FROM json_weather_data
WHERE city_id = 5128638;

-- 8e. Query the view — filter by month using DATE_TRUNC
SELECT *
FROM json_weather_data_view
WHERE DATE_TRUNC('month', observation_time) = '2018-01-01'
LIMIT 20;

-- 8f. Cross-database join: citibike trips + weather conditions by hour
SELECT
    weather     AS conditions,
    COUNT(*)    AS num_trips
FROM citibike.public.trips
LEFT OUTER JOIN json_weather_data_view
    ON DATE_TRUNC('hour', observation_time) = DATE_TRUNC('hour', starttime)
WHERE conditions IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Both databases created in this demo are reused in later labs:
--   citibike  — referenced by subsequent analytic and cloning exercises
--   weather   — referenced directly in Day4 / Lab 18 (External Tables)
-- Do NOT drop them unless the class will not continue to those labs.
-- The DROP statements are kept here for reference and commented out by default.

-- DROP DATABASE IF EXISTS citibike;   -- keep: used in later labs
-- DROP DATABASE IF EXISTS weather;    -- keep: used in Day4 Lab 18


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create objects in a dedicated student database.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Stage Exploration
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: The citibike_trips stage was created in the demo and points at a mix
--       of CSV and JSON files.  Without loading any data:
--         A) LIST the stage and use RESULT_SCAN to show only files whose
--            "name" ends in '.json' (use a LIKE filter).
--         B) How many JSON files are mixed in with the CSV files?
--            Why is this a problem for a plain COPY INTO trips?


-- Task A – YOUR CODE HERE


-- Task B – Answer in a comment:
-- There are ___ JSON files. A plain COPY fails because ...


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ File Format Options
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a new CSV file format called csv_strict in the citibike database
--       with these differences from the demo csv format:
--         - SKIP_HEADER = 1  (same as demo)
--         - ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE  (stricter than demo)
--         - NULL_IF = ('')                         (include the NULL_IF fix)
--       Then DESC the format and identify which two options differ from the
--       original csv format created in the demo.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Selective Load with PATTERN and ON_ERROR
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a new table called trips_copy in citibike.public with the same
--       column definitions as the trips table.
--       Load data from @citibike_trips into trips_copy using:
--         - your csv_strict file format
--         - PATTERN to load only .csv.gz files
--         - ON_ERROR = CONTINUE
--       After the load:
--         A) Use VALIDATE to check for any row-level errors.
--         B) Query trips_copy to confirm rows were loaded — return COUNT(*).


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Idempotency and JSON Querying
-- ──────────────────────────────────────────────────────────────────────────────
-- Task:
--       A) Re-run the exact COPY INTO trips_copy statement from Exercise 3
--          without truncating the table.  What is the row count now?
--          Why did it not change?
--
--       B) The weather database loaded in Demo 8 is still available.
--          Write a query against weather.public.json_weather_data that
--          extracts v:city.name::STRING as city_name and
--          v:main.temp::FLOAT - 273.15 as temp_celsius,
--          filtered to WHERE v:city.id::INT = 5128638 (New York City),
--          returning the 5 coldest readings ordered by temp_celsius ASC.
--
--       Hint for B: use the three-part name weather.public.json_weather_data
--                   so no USE DATABASE is needed.


-- Task A – YOUR CODE HERE
-- Answer in a comment: row count is ___ because ...


-- Task B – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- trips_copy and csv_strict were created only for this exercise set.
-- Drop them when finished.  Do NOT drop the citibike or weather databases —
-- they are used in later labs.

DROP TABLE IF EXISTS citibike.public.trips_copy;
DROP FILE FORMAT IF EXISTS citibike.public.csv_strict;
