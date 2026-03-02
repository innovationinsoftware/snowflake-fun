/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Data Share Object
2) Reader Account
3) Secure Views
----------------------------------------------------------------------------------*/

-- Step 1 – Create a Share object
-- CREATE SHARE privilege is required
USE ROLE accountadmin;

CREATE SHARE my_share;

-- Share objects
GRANT USAGE ON DATABASE demo_db TO SHARE my_share;
GRANT USAGE ON SCHEMA demo_db.scott TO SHARE my_share;
GRANT SELECT ON TABLE demo_db.scott.emp TO SHARE my_share;


-- Step 2 – Create a Reader Account

CREATE MANAGED ACCOUNT demo_reader_account
    admin_name     = 'admin',
    admin_password = 'Passw0rd12345678',
    type           = reader;

-- Example output from account creation:
/*
{
"accountName":"DEMO_READER_ACCOUNT",
"accountLocator":"OLB26468",
"url":"https://tyixbci-demo_reader_account.snowflakecomputing.com",
"accountLocatorUrl":"https://olb26468.us-east-1.snowflakecomputing.com"
}
*/

SHOW MANAGED ACCOUNTS;

--ALTER MANAGED ACCOUNT demo_reader_account ADD SHARE my_share;

-- Add the reader account to the share
ALTER SHARE my_share ADD ACCOUNTS = OLB26468;

SHOW SHARES;
SHOW GRANTS ON SHARE my_share;
SHOW GRANTS TO SHARE my_share;

SELECT CURRENT_ACCOUNT();


-- !!! EXECUTE FROM WITHIN READER ACCOUNT !!! --

-- Step 3 – Set up the Reader Account
USE ROLE accountadmin;

SHOW SHARES;

-- Create a database in the reader account from the share
CREATE DATABASE demo_db_reader FROM SHARE DKB68178.MY_SHARE;

GRANT IMPORTED PRIVILEGES ON DATABASE demo_db_reader TO ROLE sysadmin;

USE ROLE sysadmin;

-- Create warehouse in reader account
CREATE OR REPLACE WAREHOUSE compute_xs WITH
    WAREHOUSE_SIZE   = 'XSMALL'
    WAREHOUSE_TYPE   = 'STANDARD'
    AUTO_SUSPEND     = 600
    AUTO_RESUME      = TRUE
    SCALING_POLICY   = 'STANDARD';

-- Set context
USE WAREHOUSE compute_xs;
USE SCHEMA scott;

SELECT *
FROM emp;

-- After adding a view to the share:
SELECT *
FROM analysts;


-- !!! EXECUTE FROM WITHIN PROVIDER ACCOUNT !!! --

-- Step 4 – Add more objects to the share

CREATE OR REPLACE SECURE VIEW demo_db.scott.analysts AS
SELECT
    empno,
    ename,
    deptno,
    sal,
    comm,
    hiredate
FROM demo_db.scott.emp
WHERE job = 'ANALYST';

GRANT SELECT ON VIEW demo_db.scott.analysts TO SHARE my_share;
GRANT SELECT ON TABLE demo_db.scott.dept TO SHARE my_share;

CREATE TABLE demo_db.demo_schema.trips CLONE citibike.public.trips;

GRANT USAGE ON SCHEMA demo_db.demo_schema TO SHARE my_share;
GRANT SELECT ON TABLE demo_db.demo_schema.trips TO SHARE my_share;

REVOKE SELECT ON VIEW demo_db.scott.analysts FROM SHARE my_share;


-- !!! EXECUTE FROM WITHIN READER ACCOUNT !!! --

-- Step 5 – Query shared objects from Reader Account

SELECT *
FROM demo_db_reader.scott.analysts;


-- !!! EXECUTE FROM WITHIN PROVIDER ACCOUNT !!! --

-- Step 6 – Optional: Remove accounts from share
--ALTER SHARE demo_share REMOVE ACCOUNTS = VKB39446;
