/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Clone objects
2) Cloning and Time Travel
Pre-Requisite: execute SCHEMA-SETUP-SCOTT.sql if demo_db.scott schema does not exist
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS demo_db;

USE DATABASE demo_db;

CREATE SCHEMA IF NOT EXISTS demo_schema;

USE SCHEMA demo_schema;


-- Step 2 – Basic Table Cloning

-- Cloning is a metadata operation only, no data is transferred: "zero-copy" cloning
CREATE TABLE emp_clone CLONE scott.emp;

SELECT * FROM emp_clone;

-- We can create clones of clones
CREATE TABLE emp_clone_two CLONE emp_clone;

SELECT * FROM emp_clone_two;


-- Step 3 – Database-Level Cloning

-- Easily and quickly create entire database from existing database
CREATE OR REPLACE DATABASE demo_db_clone CLONE demo_db;

USE DATABASE demo_db_clone;
USE SCHEMA scott;

-- Cloning is recursive for databases and schemas
SHOW TABLES;

SELECT * FROM dept;

-- wait for 2-3 minutes

-- Data added to cloned database table will start to store micro-partitions, incurring additional cost
INSERT INTO dept(deptno, dname, loc) VALUES (50, 'HR', 'MIAMI');

-- Cloned table
SELECT * FROM dept;

-- Source table unchanged
SELECT * FROM "DEMO_DB"."SCOTT"."DEPT";


-- Step 4 – Clone from a Point in Time using Time Travel
-- wait for ~1-2 minutes

SET offset_min = 1;

SELECT *
FROM dept AT(OFFSET => -60*$offset_min);

CREATE OR REPLACE TABLE dept_clone_time_travel CLONE dept
AT(OFFSET => -60*$offset_min);

SELECT * FROM dept_clone_time_travel;


-- Clear-down resources

DROP DATABASE demo_db_clone;
