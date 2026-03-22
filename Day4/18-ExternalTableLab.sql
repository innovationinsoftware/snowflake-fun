/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) FILE FORMAT objects for JSON
2) External stages pointing to S3
3) Querying staged files with metadata columns and VARIANT data
4) SECURE VIEWs — definition hidden from non-owner roles
5) External tables — querying S3 files as a virtual table
6) Materialized Views on external tables
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
-- The weather database was created in Lab 02 (Data Load) and is reused here.
-- This lab demonstrates how to work with data that stays in external S3 storage
-- rather than being loaded into Snowflake — a common pattern for large, infrequently
-- queried datasets or files governed by data residency requirements.

USE ROLE sysadmin;

CREATE DATABASE IF NOT EXISTS weather;
USE DATABASE weather;
USE SCHEMA public;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ FILE FORMAT and External Stage
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- An inline FILE FORMAT definition works only in COPY INTO and CREATE STAGE.
-- For querying a stage directly (SELECT from @stage) a named FILE FORMAT object
-- is required. The external stage points to the public S3 bucket used throughout
-- this class — no credentials are required because the bucket allows anonymous reads.
-- LIST with the Flow Pipe operator (->> ) is a concise way to aggregate stage metadata.

CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON';

CREATE OR REPLACE STAGE nyc_weather
    URL = 's3://snowflake-workshop-lab/weather-nyc';

LIST @nyc_weather;

LIST @nyc_weather ->> SELECT SUM("size") AS total_file_size FROM $1;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Querying Files in the External Stage
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Files in an external stage are queried with SELECT $1 … FROM @stage.
-- $1 returns the raw JSON as a STRING; TO_VARIANT($1) converts it to a VARIANT
-- so that colon-notation path extraction (v:city.name) is available downstream.
-- metadata$filename and metadata$file_row_number trace every row back to its
-- source file and position — essential for debugging semi-structured loads.

SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    $1                       AS raw_json
FROM @nyc_weather (FILE_FORMAT => 'json_format')
LIMIT 10;

SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    TO_VARIANT($1)           AS json_data
FROM @nyc_weather (FILE_FORMAT => 'json_format')
LIMIT 10;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ SECURE VIEW — Hiding the Definition
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A SECURE VIEW hides its definition (DDL) from roles that do not own it.
-- DESC VIEW, SHOW VIEWS, and GET_DDL on a secure view return no column details
-- or source SQL for non-owner roles — even if SELECT is granted.
-- This is demonstrated by switching to sysadmin and running GET_DDL: the body
-- is hidden even though SELECT was explicitly granted.

CREATE OR REPLACE SECURE VIEW vw_weather AS
SELECT
    metadata$filename        AS file_name,
    metadata$file_row_number AS row_num,
    TO_VARIANT($1)           AS v
FROM @nyc_weather (FILE_FORMAT => 'json_format');

-- Query the view with JSON path extraction
SELECT
    v:time::TIMESTAMP                 AS observation_time,
    v:city.id::INT                    AS city_id,
    v:city.name::STRING               AS city_name,
    v:city.country::STRING            AS country,
    v:city.coord.lat::FLOAT           AS city_lat,
    v:city.coord.lon::FLOAT           AS city_lon,
    v:clouds.all::INT                 AS clouds,
    (v:main.temp::FLOAT) - 273.15     AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15 AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15 AS temp_max,
    v:weather[0].main::STRING         AS weather,
    v:weather[0].description::STRING  AS weather_desc,
    v:wind.deg::FLOAT                 AS wind_dir,
    v:wind.speed::FLOAT               AS wind_speed
FROM vw_weather
WHERE city_id = 5128638
LIMIT 10;

-- 4b. Show that GET_DDL hides the definition for a grantee role
DESC VIEW vw_weather;
SHOW VIEWS LIKE 'VW_WEATHER';
SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WEATHER');

GRANT SELECT ON VIEW vw_weather TO ROLE sysadmin;

USE ROLE sysadmin;

-- Definition is hidden — only column names and types are visible
DESC VIEW vw_weather;
SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WEATHER');

USE ROLE accountadmin;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ External Table
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- An external table is a virtual table whose rows are derived from files in an
-- external stage. The data never moves into Snowflake storage — the table is
-- always read-only and reflects the current contents of the S3 path.
-- AUTO_REFRESH = FALSE is used in this demo because enabling TRUE requires
-- event notification setup (SNS/SQS) — the manual REFRESH command is used instead.
-- The defining expression (AS ($1)) maps each file row to a VARIANT column v.

USE ROLE accountadmin;

CREATE OR REPLACE EXTERNAL TABLE ext_nyc_weather (
    v VARIANT AS ($1)
)
WITH LOCATION  = @nyc_weather
FILE_FORMAT    = (FORMAT_NAME = 'json_format')
AUTO_REFRESH   = FALSE;

SELECT
    v:time::TIMESTAMP                 AS observation_time,
    v:city.id::INT                    AS city_id,
    v:city.name::STRING               AS city_name,
    v:city.country::STRING            AS country,
    (v:main.temp::FLOAT) - 273.15     AS temp_avg,
    v:weather[0].main::STRING         AS weather,
    v:wind.speed::FLOAT               AS wind_speed
FROM ext_nyc_weather
WHERE city_id = 5128638
LIMIT 10;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ Materialized View on External Table
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A Materialized View (MV) on an external table caches the query result in
-- Snowflake storage and refreshes automatically when the external table changes
-- (via AUTO_REFRESH or manual REFRESH). This dramatically speeds up repeated
-- queries on external data by eliminating repeated S3 scans.
-- MVs require ENTERPRISE edition or higher.

CREATE OR REPLACE MATERIALIZED VIEW mv_nyc_weather AS
SELECT
    v:time::TIMESTAMP                 AS observation_time,
    v:city.id::INT                    AS city_id,
    v:city.name::STRING               AS city_name,
    v:city.country::STRING            AS country,
    (v:main.temp::FLOAT) - 273.15     AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15 AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15 AS temp_max,
    v:weather[0].main::STRING         AS weather,
    v:weather[0].description::STRING  AS weather_desc,
    v:wind.deg::FLOAT                 AS wind_dir,
    v:wind.speed::FLOAT               AS wind_speed
FROM ext_nyc_weather
WHERE city_id = 5128638;

SELECT * FROM mv_nyc_weather LIMIT 10;

SHOW MATERIALIZED VIEWS LIKE 'MV_NYC_WEATHER';


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The weather database is NOT dropped — it was created in Lab 02 and persists
-- as a shared resource. Individual objects may be dropped if re-created in exercises.

-- DROP DATABASE IF EXISTS weather;   -- keep: created in Lab 02


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises use the weather database created in the demo.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Stage Exploration
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Without loading any data, query @nyc_weather to find:
--         A) The total number of files in the stage (use LIST + RESULT_SCAN,
--            filter to "size" > 0).
--         B) The 3 largest files by size — return "name" and "size",
--            ordered by "size" DESC, LIMIT 3.

USE ROLE sysadmin;
USE DATABASE weather;
USE SCHEMA public;


-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Querying JSON from an External Stage
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Query @nyc_weather (FILE_FORMAT => 'json_format') to return the
--       10 hottest observations (highest temp_avg) for New York City
--       (city_id = 5128638).
--       Extract: observation_time, city_name, temp_avg (Kelvin - 273.15),
--                weather, wind_speed.
--       Order by temp_avg DESC, LIMIT 10.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ External Table Query
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using ext_nyc_weather (created in the demo), write a query that
--       returns the average, minimum, and maximum temp_avg per weather condition
--       (v:weather[0].main::STRING AS weather_condition) for city_id = 5128638.
--       Round all temperatures to 2 decimal places.
--       Order by avg_temp DESC.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — SECURE VIEW and GET_DDL
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a new SECURE VIEW called vw_wind_data in weather.public that
--       extracts from vw_weather:
--         observation_time, city_name, wind_dir, wind_speed
--       filtered to city_id = 5128638 and wind_speed > 10.
--       Then:
--         A) Run GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WIND_DATA') as accountadmin
--            — the full definition should be visible.
--         B) GRANT SELECT on vw_wind_data to ROLE sysadmin, switch to sysadmin,
--            and run GET_DDL again.
--            What is different and why?

-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop only objects created in this exercise set.
-- The weather database, stage, and ext_nyc_weather are shared resources — do not drop them.

USE ROLE accountadmin;

DROP VIEW IF EXISTS weather.public.vw_wind_data;
