/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
1) Task creation — SCHEDULE, WAREHOUSE, and AS body
2) RESUME and SUSPEND — task lifecycle management
3) Stream-triggered tasks — conditional execution with SYSTEM$STREAM_HAS_DATA
4) TASK_HISTORY — monitoring execution and state
5) Task DAGs — chaining tasks with AFTER
6) DROP TASK — cleanup
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Create a Scheduled Task
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE accountadmin;
USE WAREHOUSE demo_wh;
USE SCHEMA demo_db.public;

CREATE OR REPLACE TABLE event_log (
    event_id   INT,
    event_time TIMESTAMP,
    message    STRING
);

CREATE OR REPLACE TASK task_log_event
    WAREHOUSE = demo_wh
    SCHEDULE  = '1 MINUTE'
AS
    INSERT INTO event_log (event_id, event_time, message)
    VALUES (1, CURRENT_TIMESTAMP(), 'Heartbeat');

ALTER TASK task_log_event RESUME;

-- Wait ~2 minutes, then:
SELECT * FROM event_log ORDER BY event_time DESC;

-- [TEACHING NOTE]
-- After 2 minutes the task should have run approximately twice — but the exact
-- count depends on the task scheduler's timing tolerance (typically ±30 seconds).
-- Each INSERT adds a row with the same event_id=1 but a different event_time,
-- demonstrating that the task body executes as written on each scheduled run.
-- Discussion point: "Why might the row count after 2 minutes be 1 or 3 instead of 2?"
-- (Answer: the first run is triggered at the next full minute boundary after RESUME,
--  not immediately. If RESUME happens at :59 seconds, the first run fires at the
--  next :00, meaning you might get 1 or 2 runs in 2 minutes.)
-- Common mistake: RESUMing the task and immediately querying event_log — the
-- first run has not fired yet. Tasks are scheduled, not immediate.
-- Early finishers: add a sequence-based event_id using ROW_NUMBER or a
-- Snowflake SEQUENCE object to make each row uniquely identifiable.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Stream-Triggered Task
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE orders (
    id     INT,
    amount NUMBER
);

CREATE OR REPLACE TABLE orders_summary (
    total_orders INT,
    total_amount NUMBER,
    updated_at   TIMESTAMP
);

CREATE OR REPLACE STREAM orders_stream ON TABLE orders;

CREATE OR REPLACE TASK task_summarise_orders
    WAREHOUSE = demo_wh
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    INSERT INTO orders_summary (total_orders, total_amount, updated_at)
    SELECT
        COUNT(*)            AS total_orders,
        SUM(amount)         AS total_amount,
        CURRENT_TIMESTAMP() AS updated_at
    FROM orders_stream
    WHERE METADATA$ACTION = 'INSERT';

ALTER TASK task_summarise_orders RESUME;

-- Insert first batch and wait ~1 minute
INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200), (3, 300);

SELECT * FROM orders_summary;

-- Insert second batch and wait ~1 minute
INSERT INTO orders (id, amount) VALUES (4, 150), (5, 250);

SELECT * FROM orders_summary;

-- [TEACHING NOTE]
-- WHEN SYSTEM$STREAM_HAS_DATA('orders_stream') is a stream-triggered condition —
-- the task only runs (and charges for a warehouse second) when the stream
-- contains unprocessed rows. Without this condition, the task would fire every
-- minute and run a SELECT on an empty stream, wasting compute.
-- After the first batch: orders_summary should have 1 row with total_orders=3, total_amount=600.
-- After the second batch: a second row with total_orders=2, total_amount=400.
-- Discussion point: "What would happen if you omitted the WHERE METADATA$ACTION = 'INSERT'
-- filter and an UPDATE occurred on the orders table?"
-- (Answer: the DELETE row from the UPDATE would be included in the SUM, producing
--  incorrect totals — the old amount would be counted as a negative contribution.)
-- Common mistake: inserting data and immediately querying orders_summary — the
-- task fires on a schedule, not on the insert event. Always wait the full minute.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ TASK_HISTORY Query
-- ──────────────────────────────────────────────────────────────────────────────

SELECT
    SCHEDULED_TIME,
    COMPLETED_TIME,
    STATE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_LOG_EVENT'))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- [TEACHING NOTE]
-- STATE values: SUCCEEDED — completed without error; FAILED — error in task body;
-- SKIPPED — WHEN condition was FALSE (stream empty); SCHEDULED — not yet run.
-- ERROR_MESSAGE is NULL for successful runs; for failed runs it contains the
-- Snowflake error message, which is the primary debugging tool for task failures.
-- Discussion point: "How would you set up an alert when a task fails?"
-- (Answer: use a Snowflake Alert object or query TASK_HISTORY in a monitoring
--  task and send a notification via SYSTEM$SEND_EMAIL or Slack integration.)
-- Common mistake: querying TASK_HISTORY without TASK_NAME and being overwhelmed
-- by all tasks in the account — always filter by TASK_NAME in class.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — Suspend, Modify, Resume
-- ──────────────────────────────────────────────────────────────────────────────

-- Task A: Suspend
ALTER TASK task_log_event SUSPEND;

-- Task B: Confirm suspended state
SHOW TASKS;
-- Look for state = 'suspended' in the result

-- Task C: Change schedule
ALTER TASK task_log_event SET SCHEDULE = '5 MINUTES';

-- Task D: Resume with new schedule
ALTER TASK task_log_event RESUME;

SHOW TASKS;
-- schedule column should show '5 min' or '5 MINUTES'

-- Task E: Suspend to stop running during class
ALTER TASK task_log_event SUSPEND;

-- [TEACHING NOTE]
-- A task must be in SUSPENDED state before ALTER TASK SET SCHEDULE — Snowflake
-- raises an error if you attempt to alter a running task.
-- The SHOW TASKS result includes the schedule column, state, and last run time —
-- all key operational metrics for a scheduled pipeline.
-- Discussion point: "Why must a task be suspended before it can be altered?"
-- (Answer: an active task may be mid-execution or about to execute — altering
--  it mid-run would create inconsistent state. Suspend guarantees the task
--  is not running before the definition change takes effect.)
-- Common mistake: forgetting to RESUME after ALTER SET SCHEDULE and wondering
-- why the task never fires at the new interval.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

ALTER TASK IF EXISTS demo_db.public.task_log_event        SUSPEND;
ALTER TASK IF EXISTS demo_db.public.task_summarise_orders  SUSPEND;

DROP TASK   IF EXISTS demo_db.public.task_log_event;
DROP TASK   IF EXISTS demo_db.public.task_summarise_orders;
DROP STREAM IF EXISTS demo_db.public.orders_stream;
DROP TABLE  IF EXISTS demo_db.public.event_log;
DROP TABLE  IF EXISTS demo_db.public.orders;
DROP TABLE  IF EXISTS demo_db.public.orders_summary;
