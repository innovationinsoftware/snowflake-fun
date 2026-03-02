/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) User Defined Functions (UDFs)
2) External Functions
3) Stored Procedures
4) STORED PROCEDURES with returned value
5) last_query_id function
6) result_scan function
7) TABLE function
8) Flow Pipe Operator
----------------------------------------------------------------------------------*/

-- Step 1 – Set context
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

-- Create demo database and schema
CREATE DATABASE IF NOT EXISTS demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.demo_schema;

USE SCHEMA demo_db.demo_schema;

SELECT CURRENT_SCHEMA(), CURRENT_DATABASE();


-- Step 2 – SQL UDF: Day name on a future date

-- SQL UDF to return the name of the day of the week on a date in the future
CREATE OR REPLACE FUNCTION day_name_on(num_of_days INT)
RETURNS STRING
AS
$$
    SELECT 'In ' || CAST(num_of_days AS STRING) || ' days it will be a ' || DAYNAME(DATEADD(DAY, num_of_days, CURRENT_DATE()))
$$;
-- Single quote can be used instead of dollar sign to delimit function body


-- Use the SQL UDF as part of a query
SELECT DAY_NAME_ON(100);

SET days = 100;
SELECT DAYNAME(DATEADD(DAY, $days, CURRENT_DATE())) day_of_week;

SELECT *
FROM (VALUES (100), (200), (300)) AS t(days);

WITH x AS (
    SELECT *
    FROM (VALUES (100), (200), (300)) AS t(days)
)
SELECT
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) day_of_week,
    DAY_NAME_ON(days)                           udf_dow
FROM x;

SELECT
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) day_of_week,
    DAY_NAME_ON(days)                           udf_dow
FROM (VALUES (100), (200), (300)) AS t(days);


-- Step 3 – JavaScript UDF: Day name on a future date

CREATE OR REPLACE FUNCTION js_day_name_on(num_of_days FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    const weekday = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];

    const date = new Date();
    date.setDate(date.getDate() + NUM_OF_DAYS);
    let day = weekday[date.getDay()];

    var result = 'In ' + NUM_OF_DAYS + ' days it will be a ' + day;

    return result;
$$;

-- Use the JavaScript UDF as part of a query
SELECT JS_DAY_NAME_ON(100);

SELECT
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) day_of_week,
    JS_DAY_NAME_ON(days)                        js_udf_dow
FROM (VALUES (100), (200), (300)) AS t(days);


-- Step 4 – Overloading JavaScript UDF (all UDF languages can be overloaded)

CREATE OR REPLACE FUNCTION js_day_name_on(num_of_days FLOAT, is_abbr BOOLEAN)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (IS_ABBR === 1){
        var weekday = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
    } else {
        var weekday = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
    }

    const date = new Date();
    date.setDate(date.getDate() + NUM_OF_DAYS);

    let day = weekday[date.getDay()];

    var result = 'In ' + NUM_OF_DAYS + ' days it will be a ' + day;

    return result;
$$;

-- Use the overloaded JavaScript UDF
SELECT JS_DAY_NAME_ON(100, TRUE);
SELECT JS_DAY_NAME_ON(100, FALSE);

SELECT
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) day_of_week,
    JS_DAY_NAME_ON(days, use_abbr)              udf_dow
FROM (VALUES (100, TRUE), (200, FALSE), (300, TRUE)) AS t(days, use_abbr);

SET use_abbr = TRUE;

SELECT
    JS_DAY_NAME_ON(days, $use_abbr) js_udf_dow,
    JS_DAY_NAME_ON(days)            udf_dow
FROM (VALUES (100), (200), (300)) AS t(days);


-- Step 5 – Python UDF: Day name on a future date

CREATE OR REPLACE FUNCTION py_day_name_on(num_of_days INT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'calculate_day_name'
AS
$$
def calculate_day_name(num_of_days):
    import datetime
    from datetime import timedelta
    today = datetime.date.today()
    future_date = today + timedelta(days=num_of_days)
    day_name = future_date.strftime('%A')
    return f'In {num_of_days} days it will be a {day_name}'
$$;

SELECT PY_DAY_NAME_ON(100) py_dow;

SELECT
    DAYNAME(DATEADD(DAY, days, CURRENT_DATE())) day_of_week,
    JS_DAY_NAME_ON(days)                        js_udf_dow,
    PY_DAY_NAME_ON(days)                        py_udf_dow
FROM (VALUES (100), (200), (300)) AS t(days);


-- Step 6 – External Function (illustrative only — requires API integration setup)

/*
CREATE OR REPLACE API INTEGRATION demonstration_external_api_integration_01
    API_PROVIDER        = aws_api_gateway
    API_AWS_ROLE_ARN    = 'arn:aws:iam::123456789012:role/my_cloud_account_role'
    API_ALLOWED_PREFIXES = ('https://xyz.execute-api.us-west-2.amazonaws.com/production')
    ENABLED             = TRUE;

CREATE OR REPLACE EXTERNAL FUNCTION local_echo(string_col VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = demonstration_external_api_integration_01 -- API Integration object
    AS 'https://xyz.execute-api.us-west-2.amazonaws.com/production/remote_echo'; -- Proxy service URL

SELECT my_external_function(34, 56);
*/


-- Step 7 – Stored Procedure: Truncate all tables in a schema (JavaScript)

-- Create demo tables and insert data to test procedure
CREATE TABLE IF NOT EXISTS demo_table1
(
    name STRING,
    age  INT
);

CREATE OR REPLACE TABLE demo_table2
(
    name STRING,
    age  INT
);

INSERT INTO demo_table1 VALUES ('Joe',51),('Tom',33),('Clark',52),('Ruth',40),('Lora',23),('Ken',29);
INSERT INTO demo_table2 VALUES ('Joe',51),('Tom',33),('Clark',52),('Ruth',40),('Lora',23),('Ken',29);

SELECT COUNT(*) FROM demo_table1;
SELECT COUNT(*) FROM demo_table2;

SHOW TABLES IN demo_schema;
SHOW TABLES IN demo_db.demo_schema;

SHOW FUNCTIONS IN demo_schema;


CREATE OR REPLACE PROCEDURE truncate_all_tables_in_schema(database_name STRING, schema_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER -- can also be executed as 'caller'
AS
$$
    var result = [];
    var namespace = DATABASE_NAME + '.' + SCHEMA_NAME;
    var sql_command = 'SHOW TABLES in ' + namespace;
    var result_set = snowflake.execute({sqlText: sql_command});
    while (result_set.next()) {
        var table_name = result_set.getColumnValue(2);
        var truncate_result = snowflake.execute({sqlText: 'TRUNCATE TABLE ' + table_name});
        result.push(namespace + '.' + table_name + ' has been sucessfully truncated.');
    }
    return result.join("\n");
$$;

-- Calling a stored procedure cannot be used as part of a SQL statement, dissimilar to a UDF
CALL truncate_all_tables_in_schema('DEMO_DB', 'DEMO_SCHEMA');

SELECT COUNT(*) FROM demo_table1;
SELECT COUNT(*) FROM demo_table2;

SHOW TABLES IN SCHEMA demo_db.demo_schema;


-- Step 8 – Stored Procedure with return value

CREATE OR REPLACE PROCEDURE concat_strings(s1 STRING, s2 STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER -- can also be executed as 'caller'
AS
$$
    var result = S1 + S2;
    return result;
$$;

CALL concat_strings('abc-', 'xyz');

SET qid = (SELECT LAST_QUERY_ID());

SELECT *
FROM TABLE(RESULT_SCAN($qid));

SET result = (SELECT concat_strings FROM TABLE(RESULT_SCAN($qid)));

SELECT $result;

CALL concat_strings($result, '-ddd');


-- 1. Chaining calls using LAST_QUERY_ID
CALL concat_strings('abc-', 'xyz');
SET result = (SELECT concat_strings FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
CALL concat_strings($result, '-ddd');


SELECT $1
FROM TABLE(RESULT_SCAN($qid));

-- 2. Chaining calls using positional column reference
CALL concat_strings('abc-', 'xyz');
SET result = (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
CALL concat_strings($result, '-ddd');


SELECT
    $1,
    $2
FROM citibike.public.trips
LIMIT 10;

SELECT *
FROM citibike.public.trips
LIMIT 10;


-- Step 9 – Stored Procedure with return value (repeat / recap section)

CREATE OR REPLACE PROCEDURE concat_strings(s1 STRING, s2 STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER -- can also be executed as 'caller'
AS
$$
    var result = S1 + S2;
    return result;
$$;

CALL concat_strings('abc-', 'xyz');

SET qid = (SELECT LAST_QUERY_ID());

SELECT *
FROM TABLE(RESULT_SCAN($qid));

SET result = (SELECT concat_strings FROM TABLE(RESULT_SCAN($qid)));

SELECT $result;

CALL concat_strings($result, '-ddd');


CALL concat_strings('abc-', 'xyz');
SET result = (SELECT concat_strings FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
CALL concat_strings($result, '-ddd');


SELECT $1
FROM TABLE(RESULT_SCAN($qid));

CALL concat_strings('abc-', 'xyz');
SET result = (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
CALL concat_strings($result, '-ddd');


SELECT
    $1,
    $2
FROM citibike.public.trips
LIMIT 10;

SELECT *
FROM citibike.public.trips
LIMIT 10;


-- Clear objects
--DROP DATABASE demo_db;
