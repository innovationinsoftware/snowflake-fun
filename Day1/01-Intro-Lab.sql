/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Defining context
2) DESC and SHOW commands
3) Using shared (sample) databases
4) Account metadata functions
5) RESULT_SCAN and the Flow Pipe Operator (->>)
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Setting Session Context
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Every Snowflake session has four context values:
--   ROLE, WAREHOUSE, DATABASE, SCHEMA
-- Until all four are set, object references must be fully qualified.
-- Run Step 2 before and after Step 1 to show the "context matters" effect.

USE DATABASE snowflake_sample_data;
USE SCHEMA tpcds_sf100tcl;


-- DEMO 2 │ Inspecting a Table with DESC
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- DESC TABLE returns column names, data types, nullable flag, default values,
-- and primary key info. SHOW TABLES gives object-level metadata (owner, row
-- count, created date) — a useful contrast. Both commands are free: no compute used.

DESC TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";


-- DEMO 3 │ Context-based vs. Fully Qualified Queries
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Both queries return identical results. The unqualified version only works
-- because USE DATABASE and USE SCHEMA set the context above. With the wrong
-- context, the unqualified query fails with "Object does not exist".

-- 3a. Using the current context (short form)
SELECT
    cc_name,
    cc_manager
FROM call_center;

-- 3b. Using the fully qualified three-part name
SELECT
    cc_name,
    cc_manager
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";


-- DEMO 4 │ Sampling Another Table
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- SNOWFLAKE_SAMPLE_DATA contains many schemas and tables.
-- LIMIT 10 keeps the demo fast — no full table scan needed.
-- Discussion point: "Why does LIMIT not guarantee the same 10 rows every time?"
-- (Answer: no ORDER BY, so the optimizer may return any 10 micro-partitions.)

SELECT *
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER_DEMOGRAPHICS"
LIMIT 10;


-- DEMO 5 │ Role Switch and Warehouse Grant
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- In trial accounts, compute_wh is owned by ACCOUNTADMIN, not SYSADMIN.
-- We must grant it before switching roles, otherwise the warehouse is invisible.
-- This is a practical example of the USAGE privilege chain students will see
-- in the Security module: you need USAGE on a warehouse to use it.

GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh TO ROLE sysadmin;
USE ROLE sysadmin;


-- DEMO 6 │ Account Metadata — Building Up Step by Step
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The four sub-steps build intuition in sequence:
--   6a → single scalar function
--   6b → SHOW + RESULT_SCAN pattern (key Snowflake idiom)
--   6c → filter RESULT_SCAN to just the columns we care about
--   6d → add a computed column, then UNPIVOT into key/value rows
-- SHOW commands are metadata-only, zero compute cost.

-- 6a. Simplest form — scalar context function
SELECT CURRENT_ACCOUNT();

-- 6b. SHOW ACCOUNTS then read all columns via RESULT_SCAN
SHOW ACCOUNTS;

SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 6c. Select only the columns we need
SHOW ACCOUNTS;

SELECT
    "organization_name",
    "account_name",
    "account_locator",
    "account_url",
    "account_locator_url"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 6d. Add Snowsight URL and UNPIVOT to a readable key / value list
SHOW ACCOUNTS;

SELECT *
FROM (
    SELECT
        "organization_name",
        "account_name",
        "account_locator",
        "account_url",
        "account_locator_url",
        'https://app.snowflake.com/'
            || LOWER("organization_name") || '/'
            || LOWER("account_name") AS "snowsight_url"
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


-- DEMO 7 │ The Flow Pipe Operator (->>)
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The ->> operator (introduced in 2024) pipes the result of a SHOW command
-- directly into a SELECT as $1, eliminating the RESULT_SCAN boilerplate.
-- Demo 6d and Demo 7 produce the same result — the syntax is simply cleaner.
-- $1 is the implicit result-set alias; columns still use double-quoted names.

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
        'https://app.snowflake.com/'
            || LOWER("organization_name") || '/'
            || LOWER("account_name") AS "snowsight_url"
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


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  All exercises are READ-ONLY — no CREATE, INSERT, UPDATE, or DROP required.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Session Context
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a single SELECT statement that returns all four session context
--       values in one row with the column aliases shown below:
--       current_role | current_warehouse | current_database | current_schema


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Exploring a Sample Schema
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Set your context to the TPCH_SF1 schema inside SNOWFLAKE_SAMPLE_DATA
--       (different from the demo schema), then run SHOW TABLES to see
--       what tables are available.


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ DESC on a New Table
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using the TPCH_SF1 schema you just set, describe the ORDERS table.
--       Identify: how many columns does it have, and which column is the
--       primary key?  (Hint: look at the "primary key" column in the output.)


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ Querying with Context vs. Fully Qualified Name
-- ──────────────────────────────────────────────────────────────────────────────
-- Task A: Query the CUSTOMER table using ONLY the short (unqualified) name —
--         rely on the context you set in Exercise 2.
--         Return: C_CUSTKEY, C_NAME, C_NATIONKEY   LIMIT 5

-- Task B: Write the same query using the fully qualified three-part name.
--         Both queries must return identical results.


-- Task A – YOUR CODE HERE


-- Task B – YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 5 │ RESULT_SCAN Pattern
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Run SHOW SCHEMAS IN DATABASE snowflake_sample_data, then use
--       RESULT_SCAN(LAST_QUERY_ID()) to display only the "name" and
--       "created_on" columns from the result.
--       How many schemas does the sample database contain?


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 6 │ Flow Pipe Operator
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Rewrite your Exercise 5 solution using the ->> operator instead of
--       RESULT_SCAN, selecting the same two columns ("name" and "created_on").


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 7 │ CHALLENGE — Build Your Account Summary Card
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Using the ->> operator and SHOW ACCOUNTS, produce a key/value result
--       that contains exactly these five rows (key column / value column):
--
--       key                  │ value
--       ─────────────────────┼────────────────────────────────────────
--       organization_name    │ <your org>
--       account_name         │ <your account>
--       account_locator      │ <your locator>
--       snowsight_url        │ https://app.snowflake.com/<org>/<acct>
--       current_user         │ <result of CURRENT_USER()>
--
--       Hint: add CURRENT_USER() as a computed column before the UNPIVOT,
--             just like "snowsight_url" was added in the demo.


-- YOUR CODE HERE

