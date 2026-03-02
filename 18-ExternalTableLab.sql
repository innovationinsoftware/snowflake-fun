/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Reading data files from external stages
2) Secure Views
3) Variant Column
4) External Table based on multiple files in External Stage
5) Materialized View on External Table
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
-- We can review the data before importing it
USE ROLE sysadmin;

CREATE DATABASE IF NOT EXISTS weather;
USE DATABASE weather;
USE SCHEMA public;


-- Step 2 – Create File Format and External Stage

-- We must use a file format object; inline definition only works with COPY INTO and CREATE STAGE
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON';

-- Create external stage object
CREATE STAGE nyc_weather
    URL = 's3://snowflake-workshop-lab/weather-nyc';

-- Check files in the external stage
LIST @nyc_weather;

LIST @nyc_weather ->> SELECT SUM("size") total_file_size FROM $1;


-- Step 3 – Query Files in the External Stage

SELECT
    metadata$filename,
    metadata$file_row_number,
    $1 AS json_data
FROM @nyc_weather (FILE_FORMAT => 'json_format')
LIMIT 10;

SELECT
    metadata$filename   AS filename,
    metadata$file_row_number AS rnum,
    TO_VARIANT($1)      AS json_data
FROM @nyc_weather (FILE_FORMAT => 'json_format')
LIMIT 10;


-- Step 4 – SECURE View Demo

CREATE OR REPLACE SECURE VIEW vw_weather AS
SELECT
    metadata$filename        AS filename,
    metadata$file_row_number AS rnum,
    TO_VARIANT($1)           AS v
FROM @nyc_weather (FILE_FORMAT => 'json_format');

SELECT
    v:time::TIMESTAMP                   AS observation_time, -- same as CAST(v:time AS TIMESTAMP)
    v:city.id::INT                      AS city_id,
    v:city.name::STRING                 AS city_name,
    v:city.country::STRING              AS country,
    v:city.coord.lat::FLOAT             AS city_lat,
    v:city.coord.lon::FLOAT             AS city_lon,
    v:clouds.all::INT                   AS clouds,
    (v:main.temp::FLOAT) - 273.15       AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15   AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15   AS temp_max,
    v:weather[0].main::STRING           AS weather,
    v:weather[0].description::STRING    AS weather_desc,
    v:weather[0].icon::STRING           AS weather_icon,
    v:wind.deg::FLOAT                   AS wind_dir,
    v:wind.speed::FLOAT                 AS wind_speed
FROM vw_weather
WHERE city_id = 5128638
LIMIT 10;

DESC VIEW vw_weather;
SHOW VIEWS LIKE 'VW_WEATHER';

SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WEATHER');

GRANT SELECT ON VIEW vw_weather TO ROLE sysadmin;

USE ROLE sysadmin;

DESC VIEW vw_weather;
SHOW VIEWS LIKE 'VW_WEATHER';

SELECT GET_DDL('VIEW', 'WEATHER.PUBLIC.VW_WEATHER');


-- Step 5 – External Table

USE ROLE accountadmin;

CREATE OR REPLACE EXTERNAL TABLE ext_nyc_weather (
    v VARIANT AS ($1)  -- defining expression
)
WITH LOCATION  = @nyc_weather
FILE_FORMAT    = (FORMAT_NAME = 'json_format')
AUTO_REFRESH   = FALSE;  -- testing TRUE requires eventing

ALTER TABLE ext_nyc_weather RENAME TO ext_weather;

SELECT
    v:time::TIMESTAMP                   AS observation_time,
    v:city.id::INT                      AS city_id,
    v:city.name::STRING                 AS city_name,
    v:city.country::STRING              AS country,
    v:city.coord.lat::FLOAT             AS city_lat,
    v:city.coord.lon::FLOAT             AS city_lon,
    v:clouds.all::INT                   AS clouds,
    (v:main.temp::FLOAT) - 273.15       AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15   AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15   AS temp_max,
    v:weather[0].main::STRING           AS weather,
    v:weather[0].description::STRING    AS weather_desc,
    v:weather[0].icon::STRING           AS weather_icon,
    v:wind.deg::FLOAT                   AS wind_dir,
    v:wind.speed::FLOAT                 AS wind_speed
FROM ext_weather
WHERE city_id = 5128638
LIMIT 10;


-- Step 6 – Materialized View on External Table

CREATE OR REPLACE MATERIALIZED VIEW mv_nyc_weather AS
SELECT
    v:time::TIMESTAMP                   AS observation_time,
    v:city.id::INT                      AS city_id,
    v:city.name::STRING                 AS city_name,
    v:city.country::STRING              AS country,
    v:city.coord.lat::FLOAT             AS city_lat,
    v:city.coord.lon::FLOAT             AS city_lon,
    v:clouds.all::INT                   AS clouds,
    (v:main.temp::FLOAT) - 273.15       AS temp_avg,
    (v:main.temp_min::FLOAT) - 273.15   AS temp_min,
    (v:main.temp_max::FLOAT) - 273.15   AS temp_max,
    v:weather[0].main::STRING           AS weather,
    v:weather[0].description::STRING    AS weather_desc,
    v:weather[0].icon::STRING           AS weather_icon,
    v:wind.deg::FLOAT                   AS wind_dir,
    v:wind.speed::FLOAT                 AS wind_speed
FROM ext_weather
WHERE city_id = 5128638;


SELECT *
FROM mv_nyc_weather
LIMIT 10;

-- ALTER MATERIALIZED VIEW mv_nyc_weather REFRESH;

SHOW MATERIALIZED VIEWS LIKE 'MV_NYC_WEATHER';
