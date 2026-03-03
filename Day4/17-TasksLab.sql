/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Create Warehouse
2) Create Stream
3) Create Task
4) Resume Task
5) Check Task History
6) Suspend Task
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE ROLE accountadmin;

-- Create a Warehouse and Schema (if needed)
CREATE OR REPLACE WAREHOUSE demo_wh;

USE WAREHOUSE demo_wh;
USE SCHEMA demo_db.public;


-- Step 2 – Create Source and Target Tables

CREATE OR REPLACE TABLE raw_sales (
    id          INT,
    amount      NUMBER,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE processed_sales (
    id           INT,
    amount       NUMBER,
    processed_at TIMESTAMP
);


-- Step 3 – Create a Stream on the Source Table

CREATE OR REPLACE STREAM sales_stream ON TABLE raw_sales;


-- Step 4 – Create a Task to Process New Inserts

CREATE OR REPLACE TASK task_process_sales
    WAREHOUSE = demo_wh
    SCHEDULE  = '1 MINUTE'
AS
    INSERT INTO processed_sales (id, amount, processed_at)
    SELECT
        id,
        amount,
        CURRENT_TIMESTAMP()
    FROM sales_stream
    WHERE METADATA$ACTION = 'INSERT';


-- Step 5 – Activate the Task

ALTER TASK task_process_sales RESUME;


-- Step 6 – Insert Sample Data

INSERT INTO raw_sales (id, amount) VALUES (1, 100), (2, 250);

-- Wait about a minute ⏳, then check:
SELECT * FROM processed_sales;

INSERT INTO raw_sales (id, amount) VALUES (3, 300), (4, 250);

-- Wait about a minute ⏳, then check:
SELECT * FROM processed_sales;


-- Step 7 – Check the Task History

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_PROCESS_SALES'))
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;

SHOW TASKS;


-- Step 8 – Suspend the Task

ALTER TASK task_process_sales SUSPEND;

SHOW TASKS; -- state=suspended


-- Step 9 – Drop the Task

DROP TASK task_process_sales;

