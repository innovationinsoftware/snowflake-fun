/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Creating a SHARE object
2) Granting database, schema, and table access to a share
3) Creating a Managed Reader Account
4) Adding a reader account to a share
5) Adding SECURE VIEWs to a share
6) Revoking and modifying share content
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Create a Share Object and Grant Table Access
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A SHARE is a named object that packages database objects for consumption by
-- other Snowflake accounts. Only ACCOUNTADMIN can create shares.
-- Granting follows a hierarchy: database USAGE → schema USAGE → object SELECT.
-- All three grants are required — omitting any one prevents the consumer
-- from seeing the object.

USE ROLE accountadmin;

CREATE OR REPLACE SHARE my_share;

GRANT USAGE  ON DATABASE demo_db          TO SHARE my_share;
GRANT USAGE  ON SCHEMA   demo_db.scott    TO SHARE my_share;
GRANT SELECT ON TABLE    demo_db.scott.emp TO SHARE my_share;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Create a Reader Account
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A Managed Reader Account is a fully managed Snowflake account provisioned
-- by the provider. Reader accounts can only consume shares — they cannot create
-- their own data or share with others.
-- The account locator is captured dynamically via RESULT_SCAN to avoid
-- hard-coding an account name that varies per environment.

DROP MANAGED ACCOUNT IF EXISTS demo_reader_account;

CREATE MANAGED ACCOUNT demo_reader_account
    admin_name     = 'admin',
    admin_password = 'Passw0rd12345678',
    type           = reader;

SHOW MANAGED ACCOUNTS;

SET acc_loc_qid = (SELECT LAST_QUERY_ID());

SET acc_loc = (
    SELECT "account_locator"
    FROM TABLE(RESULT_SCAN($acc_loc_qid))
    WHERE "account_name" = 'DEMO_READER_ACCOUNT'
);

-- 2b. Add the reader account to the share
ALTER SHARE my_share ADD ACCOUNTS = IDENTIFIER($acc_loc);

-- 2c. Retrieve the login URL for the reader account
SELECT "account_locator_url"
FROM TABLE(RESULT_SCAN($acc_loc_qid))
WHERE "account_name" = 'DEMO_READER_ACCOUNT';


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Reader Account Setup (separate worksheet)
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Open the account_locator_url from Demo 2c in a new browser tab.
-- Log in as the admin user and execute 22-DataShare-ReaderAccount.sql
-- in that separate Snowflake session to complete the consumer-side setup.
-- Return to this script after the reader account demo is complete.


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Add More Objects to the Share
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Only SECURE views (not regular views) can be added to a share — this
-- prevents consumers from seeing the view's underlying SQL definition.
-- Tables from different schemas can be added as long as USAGE is granted on
-- each schema. REVOKE removes an object from the share without dropping it.

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

GRANT SELECT ON VIEW  demo_db.scott.analysts TO SHARE my_share;
GRANT SELECT ON TABLE demo_db.scott.dept     TO SHARE my_share;

-- 4b. Share a cloned citibike table from demo_schema
CREATE TABLE IF NOT EXISTS demo_db.demo_schema.trips CLONE citibike.public.trips;

GRANT USAGE  ON SCHEMA demo_db.demo_schema          TO SHARE my_share;
GRANT SELECT ON TABLE  demo_db.demo_schema.trips    TO SHARE my_share;

-- 4c. Revoke a view from the share
REVOKE SELECT ON VIEW demo_db.scott.analysts FROM SHARE my_share;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Remove Reader Account from Share
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Removing an account from a share immediately revokes all data access for
-- that consumer — active queries in the reader account fail at the next
-- metadata refresh. The SHARE object itself remains and can be re-populated.

ALTER SHARE my_share REMOVE ACCOUNTS = $acc_loc;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- demo_db must NOT be dropped — it is used throughout this lab.
-- Drop the reader account to avoid ongoing managed account charges.

-- DROP MANAGED ACCOUNT IF EXISTS demo_reader_account;  -- uncomment to clean up
-- DROP SHARE IF EXISTS my_share;                       -- uncomment to clean up


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create share objects in the provider account.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Create a Share and Grant Access
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a new share called student_share.
--       Grant access to these objects in demo_db.scott:
--         - USAGE on database demo_db
--         - USAGE on schema demo_db.scott
--         - SELECT on table demo_db.scott.dept
--       Verify the grants with SHOW GRANTS TO SHARE student_share.

USE ROLE accountadmin;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Add a Secure View to the Share
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a SECURE VIEW called demo_db.scott.dept_summary that returns:
--         deptno, dname, loc, employee_count
--       where employee_count = COUNT(*) from demo_db.scott.emp grouped by deptno.
--       Grant SELECT on this view to student_share.
--       Verify it appears in SHOW GRANTS TO SHARE student_share.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Revoke and Inspect
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Revoke SELECT on demo_db.scott.dept from student_share.
--       Run SHOW GRANTS TO SHARE student_share and confirm only dept_summary
--       (the secure view) remains.
--       In a comment, explain why a regular view cannot be added to a share.

-- YOUR CODE HERE

-- Answer:


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Dynamic Share Inspection
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Run SHOW SHARES to list all shares in the account.
--       Then use RESULT_SCAN to filter to shares where "kind" = 'OUTBOUND'
--       and return "name", "database_name", "created_on".
--       How many outbound shares are in the account?

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop the secure view and the student share.
-- demo_db must NOT be dropped.

DROP VIEW  IF EXISTS demo_db.scott.dept_summary;
DROP SHARE IF EXISTS student_share;
