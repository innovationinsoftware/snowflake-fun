/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Creating a SHARE object
2) Granting database, schema, and table access to a share
3) Creating a Managed Reader Account
4) Adding a reader account to a share
5) Adding SECURE VIEWs to a share
6) Revoking and modifying share content
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Create a Share and Grant Access
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE accountadmin;

CREATE OR REPLACE SHARE student_share;

GRANT USAGE  ON DATABASE demo_db          TO SHARE student_share;
GRANT USAGE  ON SCHEMA   demo_db.scott    TO SHARE student_share;
GRANT SELECT ON TABLE    demo_db.scott.dept TO SHARE student_share;

SHOW GRANTS TO SHARE student_share;

-- [TEACHING NOTE]
-- SHOW GRANTS TO SHARE lists every object included in the share and the
-- privilege type (USAGE or SELECT). The hierarchy must be complete:
-- database USAGE + schema USAGE + object SELECT — any missing layer means
-- the consumer cannot navigate to the object.
-- Discussion point: "What happens if you grant SELECT on the table but forget
-- to grant USAGE on the schema?"
-- (Answer: the consumer sees the database in SHOW DATABASES but the schema
--  is invisible — the table query fails with "Object does not exist.")
-- Common mistake: running GRANT as SYSADMIN — only ACCOUNTADMIN can manage
-- shares. Attempting the grant as a lower-privileged role raises:
--   "Insufficient privileges to operate on share."


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Add a Secure View to the Share
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE SECURE VIEW demo_db.scott.dept_summary AS
SELECT
    d.deptno,
    d.dname,
    d.loc,
    COUNT(e.empno) AS employee_count
FROM demo_db.scott.dept d
LEFT JOIN demo_db.scott.emp e ON d.deptno = e.deptno
GROUP BY d.deptno, d.dname, d.loc;

GRANT SELECT ON VIEW demo_db.scott.dept_summary TO SHARE student_share;

SHOW GRANTS TO SHARE student_share;

-- [TEACHING NOTE]
-- Only SECURE views can be added to a share — a regular view (CREATE VIEW)
-- raises: "Non-secure views cannot be shared."
-- The SECURE keyword hides the view's definition from non-owner roles,
-- preventing consumers from reverse-engineering the underlying SQL.
-- Discussion point: "Why does Snowflake require views in a share to be SECURE?"
-- (Answer: a consumer with SELECT on a regular view could use GET_DDL to see
--  exactly which tables and columns are being queried, potentially revealing
--  the provider's data model, filters, or business logic.)
-- Common mistake: creating the view without SECURE and attempting GRANT SELECT —
-- Snowflake rejects the grant immediately with a clear error message.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Revoke and Inspect
-- ──────────────────────────────────────────────────────────────────────────────

REVOKE SELECT ON TABLE demo_db.scott.dept FROM SHARE student_share;

SHOW GRANTS TO SHARE student_share;

-- Answer: A regular (non-secure) view cannot be added to a share because its
-- definition is visible to any role granted SELECT. If a consumer could query
-- the view definition via GET_DDL, they would see the provider's table names,
-- column names, JOIN conditions, and WHERE filters — which may contain sensitive
-- business logic or reveal the full data model. SECURE views hide this
-- information, ensuring the consumer can only see the data, not how it is derived.

-- [TEACHING NOTE]
-- After the REVOKE, SHOW GRANTS TO SHARE should list only:
--   USAGE on demo_db, USAGE on demo_db.scott, SELECT on view dept_summary.
-- The demo_db.scott.dept table row disappears from the grants list.
-- Discussion point: "Does revoking SELECT on the table affect the secure view
-- that joins to it?"
-- (Answer: no — the view is owned by accountadmin and executes with owner's
--  rights (EXECUTE AS OWNER by default for views). The consumer's access is
--  through the view grant, not the underlying table grant.)
-- Common mistake: confusing REVOKE FROM SHARE with DROP SHARE or REVOKE FROM ROLE
-- — REVOKE FROM SHARE removes a specific object grant from the share definition;
-- the share and the object both continue to exist.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Dynamic Share Inspection
-- ──────────────────────────────────────────────────────────────────────────────

SHOW SHARES;

SET shares_qid = (SELECT LAST_QUERY_ID());

SELECT
    "name"          AS share_name,
    "database_name" AS database,
    "created_on"    AS created_on
FROM TABLE(RESULT_SCAN($shares_qid))
WHERE "kind" = 'OUTBOUND'
ORDER BY created_on DESC;

-- [TEACHING NOTE]
-- SHOW SHARES lists both OUTBOUND (shares this account provides) and INBOUND
-- (shares this account receives from others). Filtering "kind" = 'OUTBOUND'
-- isolates the shares created in this class.
-- In a real account there may be INBOUND shares from Snowflake Marketplace
-- or data providers — those appear with "kind" = 'INBOUND'.
-- The "database_name" column shows which database's objects are exposed —
-- this is determined by the first GRANT USAGE ON DATABASE in the share setup.
-- Common mistake: running SHOW SHARES without filtering "kind" and being
-- confused by INBOUND shares the account may have subscribed to. Always
-- specify the filter when demonstrating share management.
-- Early finishers: use LIST ->> SELECT to chain SHOW SHARES with a RESULT_SCAN
-- filter in a single statement using the Flow Pipe operator.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

DROP VIEW  IF EXISTS demo_db.scott.dept_summary;
DROP SHARE IF EXISTS student_share;
