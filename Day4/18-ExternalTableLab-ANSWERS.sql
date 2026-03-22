/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) FILE FORMAT objects for JSON
2) External stages pointing to S3
3) Querying staged files with metadata columns and VARIANT data
4) SECURE VIEWs — definition hidden from non-owner roles
5) External tables — querying S3 files as a virtual table
6) Materialized Views on external tables
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

USE ROLE sysadmin;
USE DATABASE weather;
USE SCHEMA public;

-- Task A: Total number of files
LIST @nyc_weather;

SET lst_qid = (SELECT LAST_QUERY_ID());

SELECT COUNT(*) AS total_files
FROM TABLE(RESULT_SCAN($lst_qid))
WHERE "size" > 0;

-- Task B: 3 largest files
SELECT
    "name",
    "size"
FROM TABLE(RESULT_SCAN($lst_qid))
ORDER BY "size" DESC
LIMIT 3;

-- [TEACHING NOTE]
-- LIST @nyc_weather returns one row per file in the external stage. The "size"
-- column is in bytes. A filter WHERE "size" > 0 guards against directory
-- placeholder objects that S3 sometimes creates with zero byte size.
-- Discussion point: "How would you calculate the total uncompressed size
-- if the files were gzip-compressed?"
-- (Answer: the "size" column reflects the compressed file size as stored in S3.
--  The actual row count after decompression is only known after a SELECT or COPY.)
-- Common mistake: forgetting to SET lst_qid before the second query —
-- if another statement runs between LIST and RESULT_SCAN, LAST_QUERY_ID()
-- changes and RESULT_SCAN returns the wrong result.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Querying JSON from an External Stage
-- ──────────────────────────────────────────────────────────────────────────────

SELECT
    TO_VARIANT($1):time::TIMESTAMP                 AS observation_time,
    TO_VARIANT($1):city.name::STRING               AS city_name,
    (TO_VARIANT($1):main.temp::FLOAT) - 273.15     AS temp_avg,
    TO_VARIANT($1):weather[0].main::STRING         AS weather,
    TO_VARIANT($1):wind.speed::FLOAT               AS wind_speed
FROM @nyc_weather (FILE_FORMAT => 'json_format')
WHERE TO_VARIANT($1):city.id::INT = 5128638
ORDER BY temp_avg DESC
LIMIT 10;

-- [TEACHING NOTE]
-- Querying the external stage directly (not via the view or external table)
-- requires wrapping $1 with TO_VARIANT() before applying colon-notation path
-- extraction. $1 returns a raw STRING; TO_VARIANT converts it to VARIANT so
-- path extraction works.
-- Alternatively, students may query vw_weather (created in the demo) which
-- already exposes the VARIANT column v and all derived columns — that is an
-- equally correct answer.
-- Common mistake: using $1:city.id::INT directly without TO_VARIANT() —
-- colon-notation on a STRING type raises a type error in most Snowflake versions.
-- Early finishers: add MONTH(observation_time) AS obs_month and GROUP BY to find
-- the average temperature per month.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ External Table Query
-- ──────────────────────────────────────────────────────────────────────────────

SELECT
    v:weather[0].main::STRING   AS weather_condition,
    ROUND(AVG((v:main.temp::FLOAT) - 273.15), 2) AS avg_temp,
    ROUND(MIN((v:main.temp::FLOAT) - 273.15), 2) AS min_temp,
    ROUND(MAX((v:main.temp::FLOAT) - 273.15), 2) AS max_temp
FROM ext_nyc_weather
WHERE v:city.id::INT = 5128638
GROUP BY weather_condition
ORDER BY avg_temp DESC;

-- [TEACHING NOTE]
-- ext_nyc_weather exposes each file row as a VARIANT column v — no TO_VARIANT()
-- wrapper is needed here because the external table's defining expression
-- (v VARIANT AS ($1)) already performs the conversion.
-- The temperature is stored in Kelvin — subtracting 273.15 converts to Celsius.
-- Discussion point: "What are the performance implications of querying an
-- external table vs the same data loaded into a regular table?"
-- (Answer: external tables always require reading S3 on each query — there is
--  no micro-partition pruning unless a partition column is defined. A
--  materialized view caches the result and is refreshed on demand, providing
--  near-regular-table performance for repeated queries.)
-- Common mistake: applying WHERE on a VARIANT path before casting causes a
-- type mismatch — always cast before comparison: v:city.id::INT = 5128638, not
-- v:city.id = 5128638 (which compares VARIANT to INT and may return no rows).


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — SECURE VIEW and GET_DDL
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE accountadmin;

CREATE OR REPLACE SECURE VIEW weather.public.vw_wind_data AS
SELECT
    v:time::TIMESTAMP        AS observation_time,
    v:city.name::STRING      AS city_name,
    v:wind.deg::FLOAT        AS wind_dir,
    v:wind.speed::FLOAT      AS wind_speed
FROM vw_weather
WHERE v:city.id::INT = 5128638
  AND v:wind.speed::FLOAT > 10;

-- Task A: GET_DDL as owner (accountadmin) — full definition visible
SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WIND_DATA');

-- Task B: Grant SELECT, switch to sysadmin, run GET_DDL
GRANT SELECT ON VIEW weather.public.vw_wind_data TO ROLE sysadmin;

USE ROLE sysadmin;

SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WIND_DATA');

-- [TEACHING NOTE]
-- Task A: As accountadmin (the owner), GET_DDL returns the full CREATE SECURE VIEW
-- statement including the SELECT body and WHERE clause.
-- Task B: As sysadmin (a grantee), GET_DDL returns only the view name and column
-- list — the underlying SQL is hidden. This is the core security feature of
-- SECURE VIEW: consumers cannot reverse-engineer the data access logic.
-- Discussion point: "When would you use a SECURE VIEW vs a regular VIEW?"
-- (Answer: use SECURE VIEW whenever the view definition reveals sensitive
--  business logic, joins, or filter criteria that should not be visible to
--  the consumer — for example, a view that filters rows by classification level
--  or applies a salary multiplier formula.)
-- Common mistake: creating a regular view (without SECURE) and assuming the
-- definition is hidden — GET_DDL on a regular view exposes the full SQL to
-- any role that has SELECT on it, regardless of ownership.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE accountadmin;

DROP VIEW IF EXISTS weather.public.vw_wind_data;
