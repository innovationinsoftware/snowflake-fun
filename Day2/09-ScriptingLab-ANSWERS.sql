/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
-- INSTRUCTOR ANSWER KEY — DO NOT DISTRIBUTE TO STUDENTS
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
  PART 2 – STUDENT EXERCISES   *** ANSWER KEY ***

  Teaching notes are included below each answer to guide class discussion.
================================================================================
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 1 │ Anonymous Block with Conditional Logic
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Temperature classification using IF/ELSEIF/ELSE.

USE DATABASE experiments;
USE SCHEMA public;

BEGIN
    LET temperature FLOAT := 38.5;
    LET status STRING;

    IF (temperature > 37.5) THEN
        status := 'Fever';
    ELSEIF (temperature >= 36.0) THEN
        status := 'Normal';
    ELSE
        status := 'Low';
    END IF;

    RETURN status;
END;

-- [TEACHING NOTE]
-- Expected output: 'Fever' (38.5 > 37.5).
-- Students should verify the boundary cases by changing temperature to 37.5
-- (should return 'Normal') and 35.9 (should return 'Low').
-- The LET syntax is preferred over DECLARE for simple single-type variables —
-- it is more concise and the intent is immediately clear.
-- Common mistake: writing ELSEIF as ELSE IF (two words) — Snowflake Scripting
-- requires ELSEIF as a single keyword; ELSE IF starts a new nested IF block.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 2 │ Loop with Accumulator
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: Compute 6! using a WHILE loop.

BEGIN
    LET n      INT := 6;
    LET result INT := 1;

    WHILE (n >= 1) DO
        result := result * n;
        n := n - 1;
    END WHILE;

    RETURN result;
END;

-- [TEACHING NOTE]
-- Expected output: 720 (6 × 5 × 4 × 3 × 2 × 1).
-- The loop terminates when n < 1, meaning the final multiply is result * 1.
-- Common mistake: initialising result to 0 instead of 1 — multiplying by 0
-- produces 0 regardless of subsequent iterations.
-- Early finishers: modify the block to compute the sum of all integers from
-- 1 to n (which should equal n*(n+1)/2 — a good validation check).


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 3 │ Stored Procedure with Exception Handling
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: safe_divide — returns a/b or an error message if b=0.

CREATE OR REPLACE PROCEDURE safe_divide(a FLOAT, b FLOAT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result FLOAT;
BEGIN
    BEGIN
        result := a / b;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Error: cannot divide by zero';
    END;
    RETURN result;
END;
$$;

CALL safe_divide(10, 2);
CALL safe_divide(10, 0);

-- [TEACHING NOTE]
-- RETURNS VARIANT allows the procedure to return either a FLOAT or a STRING
-- depending on the execution path — useful when the return type depends on
-- whether an error occurred.
-- The inner BEGIN/EXCEPTION/END block catches only the division error;
-- the outer block continues and returns the result if no exception was raised.
-- Common mistake: placing the EXCEPTION block at the outer BEGIN level —
-- this catches ALL exceptions including any future statements added after
-- the division. Nesting the risky operation in its own inner block provides
-- precise scope control.
-- Discussion point: in a production pipeline, should you catch exceptions in
-- a stored procedure or let them propagate to the caller?
-- (Answer: catch and log for known recoverable errors; propagate for unexpected
--  errors so the calling system can handle them appropriately.)


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE 4 │ CHALLENGE — TABLE-Returning Stored Procedure
-- ──────────────────────────────────────────────────────────────────────────────
-- Task: generate_numbers(n INT) returns TABLE(value INT) via a WHILE loop.

CREATE OR REPLACE PROCEDURE generate_numbers(n INT)
RETURNS TABLE (value INT)
LANGUAGE SQL
AS
$$
DECLARE
    counter INT := 1;
    arr     ARRAY := ARRAY_CONSTRUCT();
    res     RESULTSET;
BEGIN
    WHILE (counter <= n) DO
        arr := ARRAY_APPEND(arr, counter);
        counter := counter + 1;
    END WHILE;

    res := (SELECT value::INT AS value FROM TABLE(FLATTEN(INPUT => :arr)));
    RETURN TABLE(res);
END;
$$;

-- Task A
CALL generate_numbers(5);

-- Task B
SELECT *
FROM TABLE(generate_numbers(10));

-- Task C
CALL generate_numbers(3) ->> SELECT SUM($1) AS total FROM $1;

-- [TEACHING NOTE]
-- The TEMPORARY TABLE pattern inside a SP body is the standard approach when
-- a RESULTSET must be built row-by-row (e.g. iterative logic, cursor processing).
-- For pure integer generation without a loop, GENERATOR is far more efficient —
-- this procedure is deliberately loop-based to practice the WHILE pattern.
-- Task C uses ->> to pipe the TABLE result into a SELECT that aggregates it.
-- $1 in the FROM clause refers to the SP result; the same $1 in the SELECT
-- refers to the first column of that result.
-- Common mistake: the RESULTSET assignment syntax — it must be:
--   res := (SELECT ...);   not   res = SELECT ...;
-- The parentheses and walrus-style := are both required.
-- Early finishers: modify generate_numbers to accept a start parameter so it
-- generates integers from start to start+n-1 instead of always starting at 1.


-- ──────────────────────────────────────────────────────────────────────────────
-- EXERCISE CLEANUP
-- ──────────────────────────────────────────────────────────────────────────────

-- DROP DATABASE IF EXISTS experiments;   -- uncomment when finished with this lab
DROP PROCEDURE IF EXISTS experiments.public.safe_divide(FLOAT, FLOAT);
DROP PROCEDURE IF EXISTS experiments.public.generate_numbers(INT);
