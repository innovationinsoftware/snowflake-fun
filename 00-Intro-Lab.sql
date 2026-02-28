/* ----------------Snowflake Fundamentals 4-day class Lab:---------------------------
1) Defining context
2) DESC command
3) Using Shared databases
----------------------------------------------------------------------------------*/

use database snowflake_sample_data;
use schema TPCDS_SF100TCL;

DESC TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";

select cc_name,cc_manager 
from call_center;


select cc_name,cc_manager 
from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER";

select * 
from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER_DEMOGRAPHICS" 
limit 10;


/* if you were going to use sysadmin you would have to change the permissions for the default compute_wh which used to be owned by default in trial by SYSADMIN and now is ACCOUNTADMIN role */
grant all privileges on warehouse compute_wh to role sysadmin;

use role sysadmin;

-- Getting Account Details:

SELECT CURRENT_ACCOUNT()

show accounts;

show accounts;
SELECT *
FROM TABLE(RESULT_SCAN(last_query_id()));

show accounts;
SELECT "organization_name", "account_name", "account_locator", "account_url", "account_locator_url"
FROM TABLE(RESULT_SCAN(last_query_id()));


show accounts;
SELECT * 
FROM (
  SELECT "organization_name", "account_name", "account_locator", "account_url", "account_locator_url", 
         'https://app.snowflake.com/' || LOWER("organization_name") || '/' || LOWER("account_name") AS "snowsight_url"
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
UNPIVOT (
  value FOR key IN (
    "organization_name",
    "account_name",
    "account_locator",
    "account_url",
    "account_locator_url",
    "snowsight_url"
  )
);

-- New Approach (Flow Pipe Operator ->>)

show accounts
->> 
SELECT * 
FROM (
  SELECT "organization_name", "account_name", "account_locator", "account_url", "account_locator_url", 
         'https://app.snowflake.com/' || LOWER("organization_name") || '/' || LOWER("account_name") AS "snowsight_url"
  FROM $1
)
UNPIVOT (
  value FOR key IN (
    "organization_name",
    "account_name",
    "account_locator",
    "account_url",
    "account_locator_url",
    "snowsight_url"
  )
);

