/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Custom Role
2) Streams
3) Dynamic SQL for role granting
4) Anonymous block
5) execute immediate statement
----------------------------------------------------------------------------------*/

-- Step 1 – Set context and create a dedicated role
USE ROLE accountadmin;

CREATE OR REPLACE ROLE stream_demo_role;

-- Grant privileges on a database/schema to the new role
GRANT USAGE ON DATABASE demo_db TO ROLE stream_demo_role;

GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA demo_db.public
TO ROLE stream_demo_role;

-- For existing tables
GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA demo_db.public
TO ROLE stream_demo_role;

-- For future tables
GRANT SELECT, INSERT, UPDATE, DELETE
ON FUTURE TABLES IN SCHEMA demo_db.public
TO ROLE stream_demo_role;

-- Grant privileges on the warehouse
GRANT ALL ON WAREHOUSE compute_wh TO ROLE stream_demo_role;


-- Step 2 – Grant the new role to the current user using dynamic SQL
DECLARE
    current_user_name STRING := CURRENT_USER();
BEGIN
    EXECUTE IMMEDIATE 'GRANT ROLE stream_demo_role TO USER "' || current_user_name || '"';
END;

-- Verify if the new role has been granted to the current user
-- ⚠️ This view can lag by up to 90 minutes, so not ideal for immediate feedback.
SELECT *
FROM snowflake.account_usage.grants_to_users
WHERE role = 'STREAM_DEMO_ROLE';

-- This should show which user got the new role granted
SHOW GRANTS OF ROLE stream_demo_role;


-- Step 3 – Switch to the new role
USE ROLE stream_demo_role;
USE DATABASE demo_db;
USE SCHEMA public;


-------------------------------------------------------------------------------
-- Demo 1 – Basic Stream on a Table

-- 1. Create Base Table
CREATE OR REPLACE TABLE products (
    id    INT,
    name  STRING,
    price NUMBER
);

-- 2. Make initial insert
INSERT INTO products VALUES (1, 'Socks', 9.99), (2, 'Shirt', 19.99);

-- 3. Create a Stream
CREATE OR REPLACE STREAM product_stream ON TABLE products;

-- 4. Make some changes
UPDATE products SET price = 8.99 WHERE id = 1;

DELETE FROM products
WHERE id = 2;

-- 5. Query the Stream
SELECT * FROM product_stream;


-------------------------------------------------------------------------------
-- Demo 2 – Using Stream in ETL (Insert-Only Table)

CREATE OR REPLACE TABLE sales_raw (
    id     INT,
    amount NUMBER
);

CREATE OR REPLACE TABLE sales_cleaned (
    id     INT,
    amount NUMBER
);

-- Create Append-Only Stream
CREATE OR REPLACE STREAM sales_stream ON TABLE sales_raw;

-- Insert Raw Data
INSERT INTO sales_raw VALUES (1, 100), (2, 200), (3, 300);

-- Check the stream
SELECT *
FROM sales_stream;

-- ETL Step: consume the stream into the cleaned table
INSERT INTO sales_cleaned
SELECT id, amount
FROM sales_stream
WHERE METADATA$ACTION = 'INSERT';

-- Check the stream again (should be empty after consumption)
SELECT *
FROM sales_stream;


-------------------------------------------------------------------------------
-- Demo 3 – Stream with MERGE (Upserts)

-- Base and Target tables
CREATE OR REPLACE TABLE customer_src (
    id   INT,
    name STRING
);

CREATE OR REPLACE TABLE customer_dim (
    id   INT,
    name STRING
);

-- Create a Stream
CREATE OR REPLACE STREAM customer_stream ON TABLE customer_src;

-- Insert and Update
INSERT INTO customer_src VALUES (1, 'Alice'), (2, 'Bob');
UPDATE customer_src SET name = 'Bobby' WHERE id = 2;

-- Check the stream
SELECT *
FROM customer_stream;

-- Merge using Stream
MERGE INTO customer_dim t
USING customer_stream s
    ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET name = s.name
WHEN NOT MATCHED THEN
    INSERT (id, name) VALUES (s.id, s.name);

-- Check the target
SELECT *
FROM customer_dim;

-- Check the stream (should be empty after merge)
SELECT *
FROM customer_stream;


-- Step 4 – Cleanup and review
SHOW STREAMS;

USE ROLE accountadmin;

SELECT *
FROM snowflake.account_usage.grants_to_users
WHERE role = 'STREAM_DEMO_ROLE';
