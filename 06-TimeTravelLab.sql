/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Time Travel
2) DATA_RETENTION_TIME_IN_DAYS parameter
3) Time Travel SQL extensions
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE ROLE accountadmin;


CREATE DATABASE IF NOT EXISTS demo_db;
USE DATABASE demo_db;

SHOW SCHEMAS LIKE 'DEMO_SCHEMA';

CREATE SCHEMA IF NOT EXISTS demo_schema;

USE SCHEMA demo_schema;

SHOW TABLES;

CREATE OR REPLACE TABLE dept_copy CLONE demo_db_clone.scott.dept;


-- Step 2 – Review and Adjust Retention Time Settings

-- Verify retention_time is set to default of 1
SHOW DATABASES LIKE 'DEMO_DB';

ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Verify updated retention_time
SHOW DATABASES LIKE 'DEMO_DB';

ALTER DATABASE demo_db SET DATA_RETENTION_TIME_IN_DAYS = 45;

-- Verify updated retention_time
SHOW DATABASES LIKE 'DEMO_DB';

-- Verify updated retention_time
SHOW SCHEMAS LIKE 'DEMO_SCHEMA';
SHOW SCHEMAS;

-- Verify updated retention_time
SHOW TABLES LIKE 'dept_copy';

ALTER SCHEMA demo_schema SET DATA_RETENTION_TIME_IN_DAYS = 10;
ALTER TABLE dept_copy SET DATA_RETENTION_TIME_IN_DAYS = 5;

-- Setting DATA_RETENTION_TIME_IN_DAYS to 0 effectively disables Time Travel
ALTER SCHEMA demo_schema SET DATA_RETENTION_TIME_IN_DAYS = 0;


-- Step 3 – UNDROP Demo

SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

DROP TABLE dept_copy;

SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

UNDROP TABLE dept_copy;

SHOW TABLES HISTORY;

SELECT
    "name",
    "retention_time",
    "dropped_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM dept_copy;


-- Step 4 – Time Travel Queries using AT / BEFORE

-- The AT keyword allows you to capture historical data inclusive of all changes
-- made by a statement or transaction up until that point.
TRUNCATE TABLE dept_copy;

SET trunc_qid = (SELECT LAST_QUERY_ID());

SELECT * FROM dept_copy;

-- 1. Select table as it was 3 minutes ago, expressed in seconds offset
SELECT *
FROM dept_copy
AT(OFFSET => -60*3);

-- 2. Select rows from the point in time when records were truncated
SELECT *
FROM dept_copy
AT(STATEMENT => $trunc_qid);

-- The BEFORE keyword allows you to select historical data up to,
-- but not including, any changes made by a specified statement or transaction.
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

SELECT DATEADD(minute, -2, CURRENT_TIMESTAMP());

-- 3. Select table as it was 2 minutes ago using a Timestamp
SELECT *
FROM dept_copy
AT(TIMESTAMP => DATEADD(minute, -2, CURRENT_TIMESTAMP()));

SELECT *
FROM dept_copy
AT(TIMESTAMP => DATEADD(minute, -3, CURRENT_TIMESTAMP()));


-- Step 5 – Restore Table from Time Travel

CREATE TABLE dept_copy_restored AS
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

SELECT * FROM dept_copy_restored;

DROP TABLE dept_copy;

SHOW TABLES HISTORY;

DESC TABLE dept_copy;

UNDROP TABLE dept_copy;

SELECT *
FROM dept_copy;

DROP TABLE dept_copy;

SHOW TABLES HISTORY;

-- This fails because the table no longer exists at that point in time
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid); -- this fails as the table does not exist

UNDROP TABLE dept_copy;

SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

CREATE OR REPLACE TABLE dept_copy AS
SELECT *
FROM dept_copy
BEFORE(STATEMENT => $trunc_qid);

SHOW TABLES HISTORY;


-- Clear-down resources
--DROP DATABASE demo_db;
ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS = 1;
