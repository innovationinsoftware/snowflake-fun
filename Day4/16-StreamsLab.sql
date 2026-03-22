/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Custom roles and privilege grants
2) Dynamic SQL with EXECUTE IMMEDIATE for role assignment
3) Standard streams — capturing INSERT, UPDATE, DELETE changes
4) Stream metadata columns — METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
5) ETL pattern — consuming a stream with INSERT … SELECT
6) MERGE with a stream — upsert pattern for dimension tables
7) Streams on views
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Custom Role and Privilege Setup
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A dedicated role (stream_demo_role) is created to demonstrate that streams
-- and the objects they monitor can be granted independently of SYSADMIN.
-- GRANT ... ON FUTURE TABLES ensures the role receives privileges on any tables
-- created after the grant — avoiding the need to re-grant after each CREATE TABLE.
-- Dynamic SQL via EXECUTE IMMEDIATE is used to grant the role to the current
-- user without hard-coding a username in the script.

USE ROLE accountadmin;

CREATE OR REPLACE ROLE stream_demo_role;

GRANT USAGE ON DATABASE demo_db TO ROLE stream_demo_role;

GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM
    ON SCHEMA demo_db.public
    TO ROLE stream_demo_role;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA demo_db.public
    TO ROLE stream_demo_role;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON FUTURE TABLES IN SCHEMA demo_db.public
    TO ROLE stream_demo_role;

GRANT ALL ON WAREHOUSE compute_wh TO ROLE stream_demo_role;

-- 1b. Grant role to current user dynamically
DECLARE
    current_user_name STRING := CURRENT_USER();
BEGIN
    EXECUTE IMMEDIATE 'GRANT ROLE stream_demo_role TO USER "' || current_user_name || '"';
END;

-- 1c. Verify the grant
SHOW GRANTS OF ROLE stream_demo_role;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Basic Stream — Capturing DML Changes
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A stream is a change-tracking object that records every INSERT, UPDATE, and
-- DELETE applied to its source table since the stream was last consumed.
-- Querying the stream does NOT consume it — only a DML statement (INSERT, MERGE,
-- etc.) that reads the stream inside a transaction consumes it.
-- METADATA$ACTION shows 'INSERT' or 'DELETE'; an UPDATE appears as a DELETE
-- of the old row followed by an INSERT of the new row, both with
-- METADATA$ISUPDATE = TRUE.

USE ROLE stream_demo_role;
USE DATABASE demo_db;
USE SCHEMA public;

CREATE OR REPLACE TABLE products (
    id    INT,
    name  STRING,
    price NUMBER
);

INSERT INTO products VALUES (1, 'Socks', 9.99), (2, 'Shirt', 19.99);

CREATE OR REPLACE STREAM product_stream ON TABLE products;

-- 2b. Make changes and observe the stream
UPDATE products SET price = 8.99 WHERE id = 1;

DELETE FROM products WHERE id = 2;

SELECT * FROM product_stream;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ ETL Pattern — Consuming a Stream with INSERT … SELECT
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Reading the stream inside an INSERT … SELECT statement consumes it —
-- after the transaction commits, the stream offset advances and the processed
-- rows no longer appear in the stream.
-- Filtering WHERE METADATA$ACTION = 'INSERT' is the standard pattern for
-- append-only pipelines where only new rows need to be propagated.
-- After consumption the stream returns zero rows, confirming no unprocessed
-- changes remain.

CREATE OR REPLACE TABLE sales_raw     (id INT, amount NUMBER);
CREATE OR REPLACE TABLE sales_cleaned (id INT, amount NUMBER);

CREATE OR REPLACE STREAM sales_stream ON TABLE sales_raw;

INSERT INTO sales_raw VALUES (1, 100), (2, 200), (3, 300);

-- Stream shows three new inserts
SELECT * FROM sales_stream;

-- Consume the stream into the cleaned table
INSERT INTO sales_cleaned
SELECT id, amount
FROM sales_stream
WHERE METADATA$ACTION = 'INSERT';

-- Stream is now empty
SELECT * FROM sales_stream;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ MERGE with a Stream — Upsert Pattern
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- MERGE is the standard pattern for synchronising a target dimension table from
-- a stream on a source table. WHEN MATCHED handles existing rows (updates);
-- WHEN NOT MATCHED handles new rows (inserts). Using the stream as the USING
-- source means only changed rows are processed — not the entire source table.

CREATE OR REPLACE TABLE customer_src (id INT, name STRING);
CREATE OR REPLACE TABLE customer_dim (id INT, name STRING);

CREATE OR REPLACE STREAM customer_stream ON TABLE customer_src;

INSERT INTO customer_src VALUES (1, 'Alice'), (2, 'Bob');

UPDATE customer_src SET name = 'Bobby' WHERE id = 2;

SELECT * FROM customer_stream;

MERGE INTO customer_dim AS t
USING customer_stream AS s ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET name = s.name
WHEN NOT MATCHED THEN
    INSERT (id, name) VALUES (s.id, s.name);

SELECT * FROM customer_dim;

-- Stream is now empty after MERGE consumed it
SELECT * FROM customer_stream;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Stream on a View
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Streams can be created on views as well as tables. The stream still tracks
-- changes on the underlying base table — the view acts as a filter or projection.
-- Changes to columns not selected by the view are not visible in the stream,
-- but they still advance the stream offset.

CREATE OR REPLACE VIEW vw_customer AS
SELECT id, name
FROM customer_src;

CREATE OR REPLACE STREAM customer_stream_vw ON VIEW vw_customer;

INSERT INTO customer_src(id, name) VALUES (5, 'George'), (6, 'Ringo');

SELECT * FROM customer_stream_vw;

DELETE FROM customer_src WHERE id = 5;

UPDATE customer_src SET name = 'Richard' WHERE id = 6;

SELECT * FROM customer_stream_vw;

SHOW STREAMS;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The tables and streams created in demo_db.public during this demo should be
-- cleaned up before students begin Part 2 to avoid naming conflicts.
-- demo_db itself must NOT be dropped — it is used in Lab 17 (Tasks) and Lab 20 (Cortex).

DROP STREAM IF EXISTS demo_db.public.product_stream;
DROP STREAM IF EXISTS demo_db.public.sales_stream;
DROP STREAM IF EXISTS demo_db.public.customer_stream;
DROP STREAM IF EXISTS demo_db.public.customer_stream_vw;
DROP TABLE  IF EXISTS demo_db.public.products;
DROP TABLE  IF EXISTS demo_db.public.sales_raw;
DROP TABLE  IF EXISTS demo_db.public.sales_cleaned;
DROP TABLE  IF EXISTS demo_db.public.customer_src;
DROP TABLE  IF EXISTS demo_db.public.customer_dim;
DROP VIEW   IF EXISTS demo_db.public.vw_customer;


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises create objects in demo_db.public using stream_demo_role.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Basic Stream and Metadata Columns
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a table called inventory with columns id INT, item STRING, qty INT.
--       Insert three rows, then create a stream called inventory_stream on it.
--       Delete the row where id = 2, then query the stream and return:
--         METADATA$ACTION, METADATA$ISUPDATE, id, item, qty
--       How many rows does the stream show? What is the METADATA$ACTION value?

USE ROLE stream_demo_role;
USE DATABASE demo_db;
USE SCHEMA public;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ ETL Stream Consumption
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a table called inventory_log with columns id INT, item STRING,
--       qty INT, logged_at TIMESTAMP.
--       Insert two more rows into inventory (id=4, id=5).
--       Then consume inventory_stream by INSERT INTO inventory_log … SELECT,
--       adding CURRENT_TIMESTAMP() as logged_at, filtering to ACTION = 'INSERT'.
--       Verify the stream is empty after consumption.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ MERGE with Stream
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a table called inventory_snapshot with the same columns as inventory.
--       Update the qty of id=1 in inventory to 999.
--       Insert a new row: id=6, item='Widget', qty=50.
--       Use MERGE to synchronise inventory_snapshot from inventory_stream:
--         - WHEN MATCHED: update qty
--         - WHEN NOT MATCHED: insert all columns
--       Verify inventory_snapshot contains the expected rows.

-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Stream on a View
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a view called vw_inventory_active that selects only rows from
--       inventory where qty > 0.
--       Create a stream called inventory_active_stream on this view.
--       Set the qty of id=3 to 0, then insert id=7, item='Gadget', qty=10.
--       Query inventory_active_stream and explain in a comment:
--         A) Which changes appear in the stream?
--         B) Does the update to qty=0 for id=3 appear? Why or why not?

-- YOUR CODE HERE

-- Answer A:

-- Answer B:


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- Drop only objects created in this exercise set.
-- demo_db must NOT be dropped — used in Lab 17 and Lab 20.

DROP STREAM IF EXISTS demo_db.public.inventory_stream;
DROP STREAM IF EXISTS demo_db.public.inventory_active_stream;
DROP TABLE  IF EXISTS demo_db.public.inventory;
DROP TABLE  IF EXISTS demo_db.public.inventory_log;
DROP TABLE  IF EXISTS demo_db.public.inventory_snapshot;
DROP VIEW   IF EXISTS demo_db.public.vw_inventory_active;
