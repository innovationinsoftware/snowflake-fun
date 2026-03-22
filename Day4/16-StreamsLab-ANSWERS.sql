/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
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
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Basic Stream and Metadata Columns
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE stream_demo_role;
USE DATABASE demo_db;
USE SCHEMA public;

CREATE OR REPLACE TABLE inventory (
    id   INT,
    item STRING,
    qty  INT
);

INSERT INTO inventory VALUES (1, 'Apples', 100), (2, 'Bananas', 50), (3, 'Cherries', 200);

CREATE OR REPLACE STREAM inventory_stream ON TABLE inventory;

DELETE FROM inventory WHERE id = 2;

SELECT
    METADATA$ACTION    AS action,
    METADATA$ISUPDATE  AS is_update,
    id,
    item,
    qty
FROM inventory_stream;

-- [TEACHING NOTE]
-- The DELETE produces one row in the stream: METADATA$ACTION = 'DELETE',
-- METADATA$ISUPDATE = FALSE. The stream shows the deleted row's values.
-- An UPDATE would appear as two rows: a DELETE (old values, ISUPDATE=TRUE)
-- followed by an INSERT (new values, ISUPDATE=TRUE).
-- Discussion point: "How would you tell apart a true DELETE from the DELETE
-- half of an UPDATE in a stream?"
-- (Answer: filter WHERE METADATA$ISUPDATE = FALSE to get true deletes;
--  WHERE METADATA$ISUPDATE = TRUE to get the change halves of updates.)
-- Common mistake: creating the stream BEFORE inserting data and then
-- running SELECT — the initial insert rows appear in the stream until the
-- stream is first consumed. Students often create the stream, insert data,
-- and are surprised to see INSERT rows they expected the stream to ignore.
-- The stream captures ALL changes since its offset, including the inserts
-- made after creation.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ ETL Stream Consumption
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE inventory_log (
    id        INT,
    item      STRING,
    qty       INT,
    logged_at TIMESTAMP
);

INSERT INTO inventory VALUES (4, 'Dates', 75), (5, 'Elderberries', 30);

-- Consume the stream
INSERT INTO inventory_log (id, item, qty, logged_at)
SELECT
    id,
    item,
    qty,
    CURRENT_TIMESTAMP() AS logged_at
FROM inventory_stream
WHERE METADATA$ACTION = 'INSERT';

-- Verify consumption
SELECT * FROM inventory_stream;

SELECT * FROM inventory_log;

-- [TEACHING NOTE]
-- The stream at this point contains the two new inserts (id=4, id=5) plus
-- the DELETE from Exercise 1 (id=2). The WHERE METADATA$ACTION = 'INSERT'
-- filter passes only the two new rows to inventory_log; the DELETE row
-- is also consumed (the entire stream offset advances) but not written.
-- After the INSERT … SELECT, the stream is empty — zero rows returned.
-- Common mistake: running SELECT FROM inventory_stream multiple times and
-- expecting the rows to disappear after each SELECT — they don't. Only a
-- DML statement (INSERT, MERGE, UPDATE) that reads the stream inside a
-- transaction consumes it.
-- Early finishers: insert one more row and run the INSERT … SELECT again.
-- Confirm only the new row appears in inventory_log (not the previously
-- consumed rows), demonstrating that the stream offset advanced correctly.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ MERGE with Stream
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE inventory_snapshot (
    id   INT,
    item STRING,
    qty  INT
);

UPDATE inventory SET qty = 999 WHERE id = 1;

INSERT INTO inventory VALUES (6, 'Widget', 50);

MERGE INTO inventory_snapshot AS t
USING inventory_stream AS s ON t.id = s.id
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
    UPDATE SET qty = s.qty
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
    INSERT (id, item, qty) VALUES (s.id, s.item, s.qty);

SELECT * FROM inventory_snapshot ORDER BY id;

-- [TEACHING NOTE]
-- The UPDATE on id=1 produces two stream rows: DELETE (old qty) and INSERT (new qty).
-- The MERGE WHEN MATCHED fires on the INSERT row for id=1 (ISUPDATE=TRUE),
-- updating qty to 999 in inventory_snapshot.
-- The INSERT of id=6 produces one stream row: INSERT (ISUPDATE=FALSE),
-- matched by WHEN NOT MATCHED — it is inserted into inventory_snapshot.
-- Discussion point: "Why do we filter on METADATA$ACTION = 'INSERT' in the
-- WHEN MATCHED clause?"
-- (Answer: without the filter, the DELETE half of an UPDATE would also match
--  WHEN MATCHED and attempt to update with the old (pre-update) values,
--  overwriting the new values immediately after setting them.)
-- Common mistake: omitting the AND METADATA$ACTION = 'INSERT' condition —
-- this causes the DELETE rows from updates to be processed as updates with
-- stale data, silently corrupting the target table.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Stream on a View
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW vw_inventory_active AS
SELECT id, item, qty
FROM inventory
WHERE qty > 0;

CREATE OR REPLACE STREAM inventory_active_stream ON VIEW vw_inventory_active;

UPDATE inventory SET qty = 0 WHERE id = 3;

INSERT INTO inventory VALUES (7, 'Gadget', 10);

SELECT * FROM inventory_active_stream;

-- Answer A: The stream shows two rows:
--   1. A DELETE + INSERT pair for id=3 (the UPDATE that set qty=0)
--   2. An INSERT for id=7 (the new row with qty=10)
-- Because the view filters WHERE qty > 0, setting id=3 qty to 0 effectively
-- makes that row "leave" the view — it appears as a DELETE in the stream.

-- Answer B: YES, the update to id=3 (qty=0) DOES appear in the stream —
-- as a DELETE row (the row leaves the view's result set because it no longer
-- satisfies WHERE qty > 0). A stream on a view tracks row-level changes to
-- the view's effective result set. When a row moves out of scope (fails the
-- view's WHERE condition) it appears as a DELETE; when it moves in scope it
-- appears as an INSERT.

-- [TEACHING NOTE]
-- Streams on views are particularly useful for change-data-capture on filtered
-- subsets — for example, tracking only active/non-deleted records.
-- The stream does not see the view definition; it sees the difference in the
-- view's effective result set between the stream's last offset and now.
-- Common mistake: expecting the stream to show only the changed column values
-- — streams always capture the full row, not a diff of individual columns.
-- Early finishers: restore id=3 qty back to 200 and re-query the stream.
-- The row should re-appear as an INSERT (it re-enters the view's scope).


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

DROP STREAM IF EXISTS demo_db.public.inventory_stream;
DROP STREAM IF EXISTS demo_db.public.inventory_active_stream;
DROP TABLE  IF EXISTS demo_db.public.inventory;
DROP TABLE  IF EXISTS demo_db.public.inventory_log;
DROP TABLE  IF EXISTS demo_db.public.inventory_snapshot;
DROP VIEW   IF EXISTS demo_db.public.vw_inventory_active;
