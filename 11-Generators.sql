/*----------------Snowflake Fundamentals 3-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Bonus Lab: 8 Range Generators Puzzles with multiple strategies
----------------------------------------------------------------------------------*/

-- Step 1 – Generate a list of integer values from 1 to 10

-- Strategy #1: Using TABLE(GENERATOR)
SELECT ROW_NUMBER() OVER(ORDER BY 1) value
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- Strategy #2: Using CONNECT BY
SELECT LEVEL value
FROM dual
CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
LIMIT 10;

-- Strategy #3: Using Recursive CTE
WITH x(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1
    FROM x
    WHERE x.value < 10
)
SELECT *
FROM x;

-- Strategy #4 (BONUS): Using FLATTEN
SELECT INDEX + 1 AS value
FROM TABLE(
    FLATTEN(
        INPUT => SPLIT(RTRIM(REPEAT('x|', 10), '|'), '|')
    )
);


-- Step 2 – Generate a list of 10 random integers from the range between 20 and 50

-- Strategy #1: Using TABLE(GENERATOR)
SELECT UNIFORM(20::INT, 50::INT, RANDOM()) rnd
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- Strategy #2: Using CONNECT BY
SELECT UNIFORM(20::INT, 50::INT, RANDOM()) rnd
FROM (
    SELECT LEVEL value
    FROM dual
    CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
    LIMIT 10
);

-- Strategy #3: Using Recursive CTE
WITH x(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1
    FROM x
    WHERE x.value < 10
)
SELECT UNIFORM(20::INT, 50::INT, RANDOM()) rnd
FROM x;


-- Step 3 – Generate a list of 10 random character strings of 5 characters each

-- Strategy #1: Using TABLE(GENERATOR)
SELECT UPPER(RANDSTR(5, RANDOM())) random_str
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- Strategy #2: Using CONNECT BY
SELECT UPPER(RANDSTR(5, RANDOM())) random_str
FROM (
    SELECT LEVEL value
    FROM dual
    CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
    LIMIT 10
);

-- Strategy #3: Using Recursive CTE
WITH x(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1
    FROM x
    WHERE x.value < 10
)
SELECT UPPER(RANDSTR(5, RANDOM())) random_str
FROM x;


-- Step 4 – Generate a range of 10 sequential date values starting with today's date

-- Strategy #1: Using TABLE(GENERATOR)
SELECT CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 "date"
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- Strategy #2: Using CONNECT BY
SELECT DATEADD(DAY, value, CURRENT_DATE()) "date"
FROM (
    SELECT LEVEL - 1 value
    FROM dual
    CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
    LIMIT 10
);

-- Strategy #3: Using Recursive CTE
WITH x(value) AS (
    SELECT 0
    UNION ALL
    SELECT value + 1
    FROM x
    WHERE x.value < 10
)
SELECT CURRENT_DATE() + value "date"
FROM x;

-- Strategy #4: Using Recursive CTE with date column
WITH x("date", value) AS (
    SELECT CURRENT_DATE, 1
    UNION ALL
    SELECT "date" + 1, value + 1
    FROM x
    WHERE x.value < 10
)
SELECT "date"
FROM x;


-- Step 5 – Generate a list of dates from current day till the end of the month

-- Strategy #1: Using TABLE(GENERATOR)
SELECT CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 "date"
FROM TABLE(GENERATOR(ROWCOUNT => 1 + DAY(LAST_DAY(CURRENT_DATE)) -
                                   DAY(CURRENT_DATE)));

-- Strategy #2: Using CONNECT BY
SELECT "date"
FROM (
    SELECT DATEADD(DAY, LEVEL - 1, CURRENT_DATE) "date"
    FROM dual
    CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
    LIMIT 31
)
WHERE EXTRACT(MONTH, "date") = EXTRACT(MONTH, CURRENT_DATE);

-- Strategy #3: Using Recursive CTE (MONTH check)
WITH x("date") AS (
    SELECT CURRENT_DATE
    UNION ALL
    SELECT "date" + 1
    FROM x
    WHERE MONTH("date" + 1) = MONTH(CURRENT_DATE)
)
SELECT "date"
FROM x;

-- Strategy #4: Using Recursive CTE (LAST_DAY check)
WITH x("date") AS (
    SELECT CURRENT_DATE
    UNION ALL
    SELECT "date" + 1
    FROM x
    WHERE "date" + 1 <= LAST_DAY(CURRENT_DATE)
)
SELECT "date"
FROM x;


-- Step 6 – Generate a list of dates from current day till the end of the week

-- Strategy #1: Using TABLE(GENERATOR)
SELECT
    CURRENT_DATE() + ROW_NUMBER() OVER(ORDER BY 1) - 1 "date",
    TO_CHAR("date", 'Dy')                              "day"
FROM TABLE(GENERATOR(ROWCOUNT => 1 + DAY(LAST_DAY(CURRENT_DATE, WEEK)) -
                                   DAY(CURRENT_DATE)));

-- Strategy #2: Using CONNECT BY
SELECT
    "date",
    TO_CHAR("date", 'Dy') "day"
FROM (
    SELECT DATEADD(DAY, LEVEL - 1, CURRENT_DATE) "date"
    FROM dual
    CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
    LIMIT 7
)
WHERE DATE_TRUNC(WEEK, "date") = DATE_TRUNC(WEEK, CURRENT_DATE);

-- Strategy #3: Using Recursive CTE (TRUNC check)
WITH x("date") AS (
    SELECT CURRENT_DATE
    UNION ALL
    SELECT "date" + 1
    FROM x
    WHERE TRUNC("date" + 1, 'WEEK') = TRUNC(CURRENT_DATE, 'WEEK')
)
SELECT
    "date",
    TO_CHAR("date", 'Dy') "day"
FROM x;

-- Strategy #4: Using Recursive CTE (LAST_DAY check)
WITH x("date") AS (
    SELECT CURRENT_DATE
    UNION ALL
    SELECT "date" + 1
    FROM x
    WHERE "date" + 1 <= LAST_DAY(CURRENT_DATE, WEEK)
)
SELECT
    "date",
    TO_CHAR("date", 'Dy') "day"
FROM x;


-- Step 7 – Generate a list of all month names in calendar order

-- Strategy #1: Using TABLE(GENERATOR)
SELECT TO_CHAR(ADD_MONTHS(TRUNC(CURRENT_DATE, 'Year'),
                          ROW_NUMBER() OVER(ORDER BY 1) - 1),
              'MMMM') month
FROM TABLE(GENERATOR(ROWCOUNT => 12));

-- Discussion: nested functions forming the right result
SELECT
    TRUNC(CURRENT_DATE, 'Year')                         jan,
    ADD_MONTHS(jan, ROW_NUMBER() OVER(ORDER BY 1) - 1)  mon,
    TO_CHAR(mon, 'MMMM')                                month
FROM TABLE(GENERATOR(ROWCOUNT => 12));

-- Strategy #2: Using CONNECT BY
SELECT TO_CHAR(DATE_FROM_PARTS(2000, LEVEL, 1), 'MMMM') month
FROM dual
CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
LIMIT 12;

-- Strategy #3: Using Recursive CTE
WITH x(day) AS (
    SELECT DATE_FROM_PARTS(2000, 1, 1)
    UNION ALL
    SELECT ADD_MONTHS(x.day, 1)
    FROM x
    WHERE MONTH(x.day) <= 11
)
SELECT TO_CHAR(day, 'MMMM') month
FROM x;


-- Step 8 – Generate the Alphabet list from A to Z

-- Strategy #1: Using TABLE(GENERATOR)
SELECT CHR(ASCII('A') + ROW_NUMBER() OVER(ORDER BY 1) - 1) letter
FROM TABLE(GENERATOR(ROWCOUNT => 26));

-- Strategy #2: Using CONNECT BY
SELECT CHR(ASCII('A') + LEVEL - 1) letter
FROM dual
CONNECT BY NVL(PRIOR COLUMN1, 0) = NVL(COLUMN1, 0)
LIMIT 26;

-- Strategy #3: Using Recursive CTE (character)
WITH x(letter) AS (
    SELECT 'A'
    UNION ALL
    SELECT CHR(ASCII(letter) + 1)
    FROM x
    WHERE letter < 'Z'
)
SELECT letter
FROM x;

-- Strategy #4: Using Recursive CTE (ASCII code)
WITH x(ascii_code) AS (
    SELECT ASCII('A')::INT
    UNION ALL
    SELECT x.ascii_code + 1
    FROM x
    WHERE CHR(x.ascii_code + 1) <= 'Z'
)
SELECT CHR(ascii_code) letter
FROM x;
