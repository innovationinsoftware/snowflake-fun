USE ROLE accountadmin;

-- Create warehouse in reader account
CREATE OR REPLACE WAREHOUSE compute_xs WITH
    WAREHOUSE_SIZE   = 'XSMALL'
    WAREHOUSE_TYPE   = 'STANDARD'
    AUTO_SUSPEND     = 600
    AUTO_RESUME      = TRUE
    SCALING_POLICY   = 'STANDARD';


SHOW SHARES;

-- Create a database in the reader account from the share

DECLARE
    v_owner_account STRING;
BEGIN
    SHOW SHARES;
    SELECT "owner_account" INTO v_owner_account 
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
    WHERE "name" = 'MY_SHARE';

    LET v_sql STRING:='CREATE DATABASE demo_db_reader FROM SHARE ' || v_owner_account || '.MY_SHARE';
    EXECUTE IMMEDIATE v_sql;
    RETURN 'Executed: ' || v_sql;
END;


GRANT IMPORTED PRIVILEGES ON DATABASE demo_db_reader TO ROLE sysadmin;

USE ROLE sysadmin;

-- Set context
USE WAREHOUSE compute_xs;
USE SCHEMA scott;

SELECT *
FROM emp;

-- After adding a view to the share:
SELECT *
FROM analysts;