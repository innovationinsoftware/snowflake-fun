/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Defining context
2) DESC command
3) Using Shared databases
----------------------------------------------------------------------------------*/

USE DATABASE snowflake_sample_data;
USE SCHEMA tpcds_sf100tcl;


-- Step 1 – Describe the CALL_CENTER table structure
DESC TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";


-- Step 2 – Query CALL_CENTER using current context
SELECT
    cc_name,
    cc_manager
FROM call_center;


-- Step 3 – Query CALL_CENTER using fully qualified name
SELECT
    cc_name,
    cc_manager
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";


-- Step 4 – Sample CUSTOMER_DEMOGRAPHICS data
SELECT *
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER_DEMOGRAPHICS"
LIMIT 10;


-- Grant compute_wh to sysadmin role so it can be used in subsequent steps
-- (In trial accounts compute_wh is now owned by ACCOUNTADMIN instead of SYSADMIN)
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh TO ROLE sysadmin;

USE ROLE sysadmin;


-- Step 5 – Getting Account Details

-- 1. Current account identifier
SELECT CURRENT_ACCOUNT();


-- 2. All columns from SHOW ACCOUNTS
SHOW ACCOUNTS;

SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- 3. Selected account columns
SHOW ACCOUNTS;

SELECT
    "organization_name",
    "account_name",
    "account_locator",
    "account_url",
    "account_locator_url"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- 4. Account details including Snowsight URL, unpivoted
SHOW ACCOUNTS;

SELECT *
FROM (
    SELECT
        "organization_name",
        "account_name",
        "account_locator",
        "account_url",
        "account_locator_url",
        'https://app.snowflake.com/' || LOWER("organization_name") || '/' || LOWER("account_name") AS "snowsight_url"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
UNPIVOT (
    value FOR key IN (
        "organization_name",
        "account_name",
        "account_locator",
        "account_url",
        "account_locator_url",
        "snowsight_url"
    )
);


-- Step 6 – New Approach using the Flow Pipe Operator (->>)
SHOW ACCOUNTS
->>
SELECT *
FROM (
    SELECT
        "organization_name",
        "account_name",
        "account_locator",
        "account_url",
        "account_locator_url",
        'https://app.snowflake.com/' || LOWER("organization_name") || '/' || LOWER("account_name") AS "snowsight_url"
    FROM $1
)
UNPIVOT (
    value FOR key IN (
        "organization_name",
        "account_name",
        "account_locator",
        "account_url",
        "account_locator_url",
        "snowsight_url"
    )
);
