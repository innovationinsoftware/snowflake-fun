/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Anonymous blocks — LET, DECLARE/BEGIN syntax
2) Conditional logic — IF / ELSEIF / ELSE
3) Loops — WHILE
4) Exception handling — BEGIN/EXCEPTION/WHEN OTHER
5) SQL Scripting stored procedures — LANGUAGE SQL
6) Stored procedures returning TABLE result sets
7) User Defined Table Functions (UDTFs) as an alternative to SPs
----------------------------------------------------------------------------------*/

/*
================================================================================
  PART 1 – INSTRUCTOR DEMO
  Each numbered demo illustrates one concept.  Students follow along in their
  own worksheets and are not expected to type anything until Part 2.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 1 │ Context Setup
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A dedicated EXPERIMENTS database isolates scripting objects from demo_db.
-- All anonymous blocks and scripting objects in this lab run inside this database.

CREATE OR REPLACE DATABASE experiments;

USE DATABASE experiments;
USE SCHEMA public;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 2 │ Anonymous Blocks — Variable Declaration and Assignment
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Two syntactic styles exist for variable handling inside a BEGIN/END block:
--   LET — combines declaration and assignment in one line (inside BEGIN)
--   DECLARE — declares the variable before BEGIN; assigned inside BEGIN
-- Both styles are valid; LET is more concise for simple cases.
-- Anonymous blocks are not stored — they execute once and are discarded.

-- 2a. LET style
BEGIN
    LET message STRING := 'Hello Snowflake Scripting!';
    RETURN message;
END;

-- 2b. DECLARE/BEGIN style
DECLARE
    message STRING;
BEGIN
    message := 'Hello Snowflake Scripting!';
    RETURN message;
END;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 3 │ Conditional Logic — IF / ELSEIF / ELSE
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Snowflake Scripting uses IF / ELSEIF / ELSE / END IF syntax.
-- Conditions are standard SQL boolean expressions enclosed in parentheses.
-- The block returns a scalar value via RETURN — useful for testing logic
-- before embedding it in a stored procedure.

BEGIN
    LET score  INT := 75;
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


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 4 │ Loops — WHILE
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- WHILE loops use the WHILE (condition) DO / END WHILE syntax.
-- Snowflake Scripting also supports FOR and LOOP/BREAK constructs —
-- WHILE is shown here as the most familiar to SQL developers.
-- The block accumulates a running total, demonstrating mutable variable state.

BEGIN
    LET counter INT := 1;
    LET total   INT := 0;

    WHILE (counter <= 5) DO
        total   := total + counter;
        counter := counter + 1;
    END WHILE;

    RETURN total;
END;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 5 │ Exception Handling
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Exceptions are caught with EXCEPTION / WHEN / THEN inside a nested BEGIN block.
-- WHEN OTHER catches any unhandled exception type — equivalent to a generic catch.
-- The inner BEGIN/EXCEPTION/END nesting allows specific sections of logic
-- to be wrapped independently while the outer block continues executing.

BEGIN
    LET divisor INT := 0;
    LET result  FLOAT;

    BEGIN
        result := 10 / divisor;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Error caught: division by zero';
    END;

    RETURN result;
END;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 6 │ SQL Scripting Stored Procedure — Scalar Return
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- Wrapping scripting logic inside a stored procedure makes it reusable and
-- callable by name. LANGUAGE SQL stored procedures use Snowflake Scripting
-- syntax inside the $$ body — not JavaScript or Python.

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


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 7 │ Stored Procedure Returning a TABLE Result Set
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- RETURNS TABLE(...) declares the column schema of the returned result set.
-- The RESULTSET variable captures the output of a SELECT, and RETURN TABLE(res)
-- emits it as a queryable table result.
-- Three consumption patterns are shown: CALL, TABLE(), and the ->> operator.

CREATE OR REPLACE PROCEDURE get_sales_sp(target_region STRING)
RETURNS TABLE (id INT, product STRING, price NUMBER(10,2), region STRING)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT * FROM (
            SELECT 1, 'Snowflake Pro License',      1200.00, 'North America'
            UNION ALL
            SELECT 2, 'Data Engineering Course',     450.00, 'EMEA'
            UNION ALL
            SELECT 3, 'Cloud Storage Add-on',        150.50, 'North America'
            UNION ALL
            SELECT 4, 'Consulting Session',         3000.00, 'APAC'
        ) AS sales(transaction_id, product_name, amount, region_name)
        WHERE region_name = :target_region OR :target_region = 'ALL'
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- 7a. CALL operator
CALL get_sales_sp('ALL');

-- 7b. TABLE() function in FROM clause
SELECT *
FROM TABLE(get_sales_sp('ALL'));

-- 7c. Flow Pipe operator
CALL get_sales_sp('ALL') ->> SELECT * FROM $1;


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO 8 │ User Defined Table Function (UDTF) — Alternative to SP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- A UDTF achieves the same row-returning result as a TABLE-returning SP but
-- is used in the FROM clause directly (no CALL needed).
-- The key difference: UDTFs cannot execute DDL/DML — they are read-only.
-- Use SPs when the logic needs to modify data; use UDTFs for parameterised reads.

CREATE OR REPLACE FUNCTION get_demo_sales_data(target_region STRING)
RETURNS TABLE (
    transaction_id INT,
    product_name   STRING,
    amount         NUMBER(10,2),
    region_name    STRING
)
AS
$$
    SELECT * FROM (
        SELECT 1, 'Snowflake Pro License',  1200.00, 'North America'
        UNION ALL
        SELECT 2, 'Data Engineering Course', 450.00, 'EMEA'
        UNION ALL
        SELECT 3, 'Cloud Storage Add-on',    150.50, 'North America'
        UNION ALL
        SELECT 4, 'Consulting Session',     3000.00, 'APAC'
    ) AS sales(transaction_id, product_name, amount, region_name)
    WHERE region_name = target_region OR target_region = 'ALL'
$$;

SELECT
    product_name,
    amount
FROM TABLE(get_demo_sales_data('North America'))
WHERE amount > 500;

SELECT * FROM TABLE(get_demo_sales_data('ALL'));


-- ──────────────────────────────────────────────────────────────────────────────
-- DEMO CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [INSTRUCTOR NOTE]
-- The EXPERIMENTS database is used only in this lab and can be dropped safely.
-- demo_db must NOT be dropped — it is used by all remaining Day 2 and Day 4 labs.

-- DROP DATABASE IF EXISTS experiments;   -- safe to drop after this lab


/*
================================================================================
  PART 2 – STUDENT EXERCISES
  Complete each exercise independently.  Run your query and verify the result.
  Exercises run inside the experiments database.
  Clean-up steps are provided at the end.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Anonymous Block with Conditional Logic
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write an anonymous block that:
--         - Declares a variable called temperature of type FLOAT set to 38.5
--         - Uses IF/ELSEIF/ELSE to assign a STRING variable called status:
--             'Fever'   if temperature > 37.5
--             'Normal'  if temperature >= 36.0
--             'Low'     otherwise
--         - Returns status

USE DATABASE experiments;
USE SCHEMA public;


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Loop with Accumulator
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Write an anonymous block that computes the factorial of 6 using a
--       WHILE loop.  Declare two INT variables: n (start at 6) and result
--       (start at 1).  Multiply result by n on each iteration, decrement n,
--       and stop when n < 1.  Return result.
--       Expected output: 720


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Stored Procedure with Exception Handling
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a stored procedure called safe_divide(a FLOAT, b FLOAT)
--       that returns a / b as a FLOAT, but catches a division-by-zero error
--       using EXCEPTION WHEN OTHER THEN and returns the string
--       'Error: cannot divide by zero' instead.
--       Test with: CALL safe_divide(10, 2)  → should return 5.0
--                  CALL safe_divide(10, 0)  → should return the error message


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — TABLE-Returning Stored Procedure
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Create a SQL Scripting stored procedure called generate_numbers(n INT)
--       that returns TABLE(value INT) containing integers from 1 to n.
--       Build the result set using a WHILE loop that INSERTs rows into a
--       temporary table, then returns it.
--       (Hint: use ARRAY_CONSTRUCT (preferred) or CREATE TEMPORARY TABLE inside the procedure body,
--        populate it in the loop, then RETURN TABLE(res) where res is a
--        RESULTSET DEFAULT (SELECT value FROM the temp table ORDER BY value).)
--       Test with:
--         A) CALL generate_numbers(5)                  → 1,2,3,4,5
--         B) SELECT * FROM TABLE(generate_numbers(10)) → 1..10
--         C) CALL generate_numbers(3) ->> SELECT SUM($1) FROM $1  → 6


-- YOUR CODE HERE


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────
-- [NOTE]
-- The EXPERIMENTS database was created for this lab and is safe to drop.

-- DROP DATABASE IF EXISTS experiments;   -- uncomment when finished with this lab
DROP PROCEDURE IF EXISTS experiments.public.safe_divide(FLOAT, FLOAT);
DROP PROCEDURE IF EXISTS experiments.public.generate_numbers(INT);

