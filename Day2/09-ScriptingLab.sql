/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Variables
2) Conditional logic
3) Loops
4) Exception Handling
5) Simple procedural workflows
----------------------------------------------------------------------------------*/

CREATE OR REPLACE DATABASE EXPERIMENTS;

USE DATABASE EXPERIMENTS;
USE SCHEMA public;

-- Step 1 – Anonymous Block Basics

BEGIN
	-- 'LET' is used inside the execution block. It combines declaration and initialization into a single line.
    LET message STRING := 'Hello Snowflake Scripting!';
    RETURN message;
END;

-- Alternative strategy:

DECLARE
    message STRING; -- Declared here
BEGIN
    message := 'Hello Snowflake Scripting!'; -- Assigned here
    RETURN message;
END;

-- Step 2 – Conditional Logic

BEGIN
    LET score INT := 75;
    LET result STRING;

    IF (score >= 90) THEN
        result := 'Excellent';
    ELSEIF (score >= 70) THEN
        result := 'Good';
    ELSE
        result := 'Needs Improvement';
    END IF;

    RETURN result;
END;

-- Step 3 – Loops

BEGIN
    LET counter INT := 1;
    LET total INT := 0;

    WHILE (counter <= 5) DO
        total := total + counter;
        counter := counter + 1;
    END WHILE;

    RETURN total;
END;


-- Step 4 – Exception Handling

BEGIN
    LET divisor INT := 0;
    LET result FLOAT;

    BEGIN
        result := 10 / divisor;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Error occurred: Division by zero';
    END;

    RETURN result;
END;


-- Step 5 - Stored Procedure Example:

CREATE OR REPLACE PROCEDURE demo_proc()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET x INT := 10;
    RETURN 'Value is ' || x;
END;
$$;

CALL demo_proc();


-- Step 6 - Return a result set from a stored procedure:

CREATE OR REPLACE PROCEDURE get_sales_sp(target_region STRING)
RETURNS TABLE (id INT, product STRING, price NUMBER(10,2), region STRING)
LANGUAGE SQL
AS
$$
DECLARE
    -- 1. Define the Result Set
    res RESULTSET DEFAULT (
        SELECT * FROM (
            SELECT 1, 'Snowflake Pro License', 1200.00, 'North America'
            UNION ALL
            SELECT 2, 'Data Engineering Course', 450.00, 'EMEA'
            UNION ALL
            SELECT 3, 'Cloud Storage Add-on', 150.50, 'North America'
            UNION ALL
            SELECT 4, 'Consulting Session', 3000.00, 'APAC'
        ) AS sales(transaction_id, product_name, amount, region_name)
        WHERE region_name = :target_region OR :target_region = 'ALL'
    );
BEGIN
    -- 2. Return the defined Result Set
    RETURN TABLE(res);
END;
$$;

-- Step 8: Three Ways to query the result of SP:

-- 1. Using CALL operator
CALL get_sales_sp('ALL');

-- 2. Using TABLE function in FROM clause
SELECT *
FROM TABLE(get_sales_sp('ALL'));

-- 3. Using Snowflake Pipe Operator:

CALL get_sales_sp('ALL') ->> SELECT * FROM $1;


-- Step 9: Create identical UDTF

CREATE OR REPLACE FUNCTION get_demo_sales_data(target_region STRING)
RETURNS TABLE (transaction_id INT, product_name STRING, amount NUMBER(10,2), region_name STRING)
AS
$$
    SELECT * FROM (
        SELECT 1, 'Snowflake Pro License', 1200.00, 'North America'
        UNION ALL
        SELECT 2, 'Data Engineering Course', 450.00, 'EMEA'
        UNION ALL
        SELECT 3, 'Cloud Storage Add-on', 150.50, 'North America'
        UNION ALL
        SELECT 4, 'Consulting Session', 3000.00, 'APAC'
    ) AS sales(transaction_id, product_name, amount, region_name)
    WHERE region_name = target_region OR target_region = 'ALL'
$$;


-- Querying for a specific region
SELECT 
    product_name, 
    amount 
FROM TABLE(get_demo_sales_data('North America'))
WHERE amount > 500;

-- Querying for everything
SELECT * FROM TABLE(get_demo_sales_data('ALL'));
