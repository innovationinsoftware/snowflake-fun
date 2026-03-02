/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Creating a Database
2) Schemas
3) Using External Stages
4) Copy Into Command
5) ON_ERROR attribute
6) PATTERN attribute
----------------------------------------------------------------------------------*/

-- Step 1 – Schema Preparation

USE ROLE sysadmin;

CREATE DATABASE citibike;

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


USE DATABASE citibike;
USE SCHEMA public;


-- Step 2 – Create External Stage

--CREATE STAGE citibike_trips URL = 's3://snowflake-workshop-lab/citibike-trips';

CREATE OR REPLACE STAGE citibike_trips URL = 's3://snowflake-workshop-lab/japan/citibike-trips';

LIST @citibike_trips;

SET id = (SELECT LAST_QUERY_ID());

SELECT *
FROM TABLE(RESULT_SCAN($id))
WHERE "name" LIKE '%citibike%'
LIMIT 10;


-- Step 3 – Define CSV File Format

CREATE OR REPLACE FILE FORMAT csv
    TYPE                          = CSV
    FIELD_DELIMITER               = ','
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL           = TRUE
    SKIP_HEADER                   = 1;


-- Step 4 – Data Load

USE DATABASE citibike;
USE SCHEMA public;

-- 1. This will not work since there are JSON files mingled in with the CSV now in the stage
COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv;

-- 2. Adding a PATTERN to load only CSV files and skip errors
COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR   = CONTINUE
PATTERN    = '.*[.]csv.gz';

SET id = (SELECT LAST_QUERY_ID());

SELECT * FROM TABLE(VALIDATE(trips, JOB_ID => $id));

CREATE TABLE trips_load_errors AS
SELECT * FROM TABLE(VALIDATE(trips, JOB_ID => $id));

SELECT *
FROM trips_load_errors
LIMIT 10;

SELECT *
FROM trips
LIMIT 10;

-- 3. Reload after truncate
COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR   = CONTINUE
PATTERN    = '.*[.]csv.gz';

TRUNCATE TABLE trips;

-- 4. Updated file format with NULL_IF — this attribute resolved empty-field issues
CREATE OR REPLACE FILE FORMAT csv
    TYPE                          = CSV
    FIELD_DELIMITER               = ','
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL           = TRUE
    SKIP_HEADER                   = 1
    NULL_IF                       = (''); -- THIS ATTRIBUTE MADE THE DIFFERENCE!

COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR   = CONTINUE
PATTERN    = '.*[.]csv.gz';

SET id = (SELECT LAST_QUERY_ID());

SELECT * FROM TABLE(VALIDATE(trips, JOB_ID => $id));


/*----------------Data Wrangling :---------------------------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Data Wrangling
2) Storing JSON data
3) Querying JSON data
----------------------------------------------------------------------------------*/

-- Step 5 – Setup Weather Database

USE ROLE accountadmin;
DROP DATABASE IF EXISTS weather;

GRANT ALL ON WAREHOUSE compute_wh TO sysadmin;


USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE SCHEMA public;

CREATE DATABASE IF NOT EXISTS weather;
USE DATABASE weather;


-- Step 6 – Create JSON Table and Stage

CREATE TABLE json_weather_data (v VARIANT);

CREATE STAGE nyc_weather
URL = 's3://snowflake-workshop-lab/weather-nyc';

LIST @nyc_weather;


-- Step 7 – Load JSON Data

COPY INTO json_weather_data
FROM @nyc_weather
FILE_FORMAT = (TYPE = json);

SELECT *
FROM json_weather_data
LIMIT 10;


-- Step 8 – Create Weather View

CREATE VIEW json_weather_data_view AS
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
FROM json_weather_data
WHERE city_id = 5128638;


-- Step 9 – Query Weather View

SELECT *
FROM json_weather_data_view
WHERE DATE_TRUNC('month', observation_time) = '2018-01-01'
LIMIT 20;


-- Step 10 – Join Trips and Weather Data

SELECT
    weather AS conditions,
    COUNT(*) AS num_trips
FROM citibike.public.trips
LEFT OUTER JOIN json_weather_data_view
    ON DATE_TRUNC('hour', observation_time) = DATE_TRUNC('hour', starttime)
WHERE conditions IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;


-- Cleanup (optional)
DROP DATABASE IF EXISTS weather;
