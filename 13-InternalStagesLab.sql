/*----------------Snowflake Fundamentals 3-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Stage types
2) Listing staged data files
3) PUT command
4) Querying staged data files
5) Removing staged data files
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE ROLE sysadmin;

CREATE DATABASE movies_db;
CREATE SCHEMA movies_schema;

CREATE OR REPLACE TABLE movies
(
    id           INT,
    title        STRING,
    release_date DATE
);


-- Step 2 – Internal Stages: list contents

-- List contents of user stage (contains worksheet data)
LS @~;
LIST @~;

-- List contents of table stage
LS @%movies;

-- Create internal named stage
CREATE STAGE movies_stage;

-- List contents of internal named stage
LS @movies_stage;


-- Step 3 – PUT command (execute from within SnowSQL)
-- Make sure the path does not contain a space character
USE ROLE sysadmin;
USE DATABASE movies_db;
USE SCHEMA movies_schema;

PUT file://C:\Personal\Training\movies.csv @~ AUTO_COMPRESS = FALSE;
PUT file://C:\Personal\Training\movies.csv @%movies AUTO_COMPRESS = FALSE;
PUT file://C:\Personal\Training\movies.csv @movies_stage AUTO_COMPRESS = FALSE;

LS @~/movies.csv;
LS @%movies;
LS @movies_stage;


-- Step 4 – Query staged data

-- Raw positional columns from stage
SELECT $1, $2, $3 FROM @~/movies.csv;

-- Create CSV file format to parse files in stage
CREATE FILE FORMAT csv_file_format
    TYPE        = CSV
    SKIP_HEADER = 1;

-- Metadata columns with file format
SELECT
    metadata$filename,
    metadata$file_row_number,
    $1,
    $2,
    $3
FROM @%movies (FILE_FORMAT => 'csv_file_format');

-- With PATTERN filter
SELECT
    metadata$filename,
    metadata$file_row_number,
    $1,
    $2,
    $3
FROM @movies_stage (FILE_FORMAT => 'csv_file_format', PATTERN => '.*[.]csv') t;

-- With PATH filter
SELECT
    metadata$filename,
    metadata$file_row_number,
    $1,
    $2,
    $3
FROM @~/movies.csv (FILE_FORMAT => 'csv_file_format') t;


-- Step 5 – Remove files from stage
RM @~/movies.csv;
RM @%movies;
RM @movies_stage;
-- REMOVE @~/movies.csv;
