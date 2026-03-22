/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Defining context
2) DESC and SHOW commands
3) Using shared (sample) databases
4) Account metadata functions
5) RESULT_SCAN and the Flow Pipe Operator (->>)
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Session Context
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write a single SELECT statement that returns all four session context
--       values in one row with the column aliases shown below:
--       current_role | current_warehouse | current_database | current_schema

SELECT
    CURRENT_ROLE()      AS current_role,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_DATABASE()  AS current_database,
    CURRENT_SCHEMA()    AS current_schema;

-- [TEACHING NOTE]
-- All four are scalar functions — no FROM clause needed.
-- If CURRENT_WAREHOUSE() returns NULL the virtual warehouse is not started
-- or not set. This is a common student gotcha on the first day.
-- Discussion point: what happens if a query runs with no warehouse set?
-- (Answer: Snowflake returns an error: "No active warehouse selected.")


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Exploring a Sample Schema
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Set context to TPCH_SF1, then SHOW TABLES.

USE DATABASE snowflake_sample_data;
USE SCHEMA tpch_sf1;

SHOW TABLES;

-- [TEACHING NOTE]
-- TPCH_SF1 is the TPC-H benchmark dataset at scale factor 1 (≈ 1 GB).
-- It has 8 tables: CUSTOMER, LINEITEM, NATION, ORDERS, PART, PARTSUPP,
-- REGION, SUPPLIER — a classic star/snowflake schema students will recognise.
-- TPCDS (used in the demo) is TPC-DS: a more complex retail schema.
-- Both come free with every Snowflake account via data sharing.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ DESC on a New Table
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Describe ORDERS; identify column count and primary key.

DESC TABLE orders;

-- [TEACHING NOTE]
-- ORDERS has 9 columns.  O_ORDERKEY is the primary key (shown in the
-- "primary key" output column as "Y").
-- Note that Snowflake stores primary key metadata but does NOT enforce it
-- as a constraint — it is informational only (used by query optimisers and
-- BI tools). This often surprises students coming from RDBMS backgrounds.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ Querying with Context vs. Fully Qualified Name
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A – short (unqualified) name, relying on context from Exercise 2
SELECT
    c_custkey,
    c_name,
    c_nationkey
FROM customer
LIMIT 5;

-- Task B – fully qualified three-part name
SELECT
    c_custkey,
    c_name,
    c_nationkey
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
LIMIT 5;

-- [TEACHING NOTE]
-- Both queries return the same columns; the rows may differ between runs
-- (no ORDER BY). This reinforces the point from Demo 3.
-- Discussion point: what would happen to Task A if USE SCHEMA tpcds_sf100tcl
-- were run between Exercise 2 and Exercise 4?
-- (Answer: Task A would fail because CUSTOMER does not exist in TPCDS.)
-- This is the single most common Snowflake beginner error — wrong context.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 5 │ RESULT_SCAN Pattern
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: SHOW SCHEMAS, then RESULT_SCAN for "name" and "created_on".

SHOW SCHEMAS IN DATABASE snowflake_sample_data;

SELECT
    "name",
    "created_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- [TEACHING NOTE]
-- At time of writing SNOWFLAKE_SAMPLE_DATA contains 8 schemas:
--   TPCH_SF1, TPCH_SF10, TPCH_SF100, TPCH_SF1000,
--   TPCDS_SF10TCL, TPCDS_SF100TCL, WEATHER, INFORMATION_SCHEMA
-- The exact count may change as Snowflake adds datasets.
-- Key concept: RESULT_SCAN lets you turn any SHOW output into a relational
-- result set you can filter, join, or aggregate — powerful for automation.
-- The column names from SHOW commands use double-quoted lowercase names.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 6 │ Flow Pipe Operator
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Rewrite Exercise 5 using ->>

SHOW SCHEMAS IN DATABASE snowflake_sample_data
->>
SELECT
    "name",
    "created_on"
FROM $1;

-- [TEACHING NOTE]
-- $1 is the implicit alias for the piped-in result set.
-- Functionally identical to Exercise 5 but removes the need to call
-- RESULT_SCAN and LAST_QUERY_ID() explicitly.
-- The ->> operator was introduced in Snowflake's 2024 release cycle.
-- It only works with SHOW commands as the left-hand side.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 7 │ CHALLENGE — Build Your Account Summary Card
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Use ->> and SHOW ACCOUNTS to produce a 5-row key/value card,
--       including a computed snowsight_url and CURRENT_USER() row.

SHOW ACCOUNTS
->>
SELECT *
FROM (
    SELECT
        "organization_name",
        "account_name",
        "account_locator",
        'https://app.snowflake.com/'
            || LOWER("organization_name") || '/'
            || LOWER("account_name")  AS "snowsight_url",
        CURRENT_USER()                AS "current_user"
    FROM $1
)
UNPIVOT (
    value FOR key IN (
        "organization_name",
        "account_name",
        "account_locator",
        "snowsight_url",
        "current_user"
    )
);

-- [TEACHING NOTE]
-- This exercise combines three techniques in one:
--   1. The ->> flow pipe operator
--   2. A computed column (snowsight_url) and a context function (CURRENT_USER())
--   3. UNPIVOT to produce a vertical key/value layout
-- UNPIVOT syntax from Demo 6d applies directly here. Common mistakes:
--   • Forgetting to wrap the SELECT in a subquery before UNPIVOT
--   • Listing a column in UNPIVOT that was not selected in the inner SELECT
--   • Using single-quoted column names instead of double-quoted (causes error)
-- Early finishers: extend the card by adding "account_edition" from SHOW ACCOUNTS
-- as a sixth key/value row.
