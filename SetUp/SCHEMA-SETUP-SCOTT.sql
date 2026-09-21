/*----------------Snowflake Fundamentals 4-day class Lab:---------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Create DEMO_DB database and SCOTT schema
2) Create DEPT and EMP tables
3) Insert seed data
----------------------------------------------------------------------------------*/

-- Step 1 – Create Database and Schema
CREATE DATABASE IF NOT EXISTS demo_db;
CREATE SCHEMA demo_db.scott;

USE SCHEMA demo_db.scott;


-- Step 2 – Create Tables

CREATE TABLE scott.dept
(
    deptno NUMBER(2)  CONSTRAINT pk_dept PRIMARY KEY,
    dname  VARCHAR(14),
    loc    VARCHAR(13)
);

CREATE TABLE scott.emp
(
    empno    NUMBER(4)   CONSTRAINT pk_emp PRIMARY KEY,
    ename    VARCHAR(10),
    job      VARCHAR(9),
    mgr      NUMBER(4),
    hiredate DATE,
    sal      NUMBER(7,2),
    comm     NUMBER(7,2),
    deptno   NUMBER(2)   CONSTRAINT fk_deptno REFERENCES dept
);


-- Step 3 – Insert Data

INSERT INTO scott.dept VALUES
(10, 'ACCOUNTING', 'NEW YORK'),
(20, 'RESEARCH',   'DALLAS'  ),
(30, 'SALES',      'CHICAGO' ),
(40, 'OPERATIONS', 'BOSTON'  );

INSERT INTO scott.emp VALUES
(7369, 'SMITH',  'CLERK',     7902, TO_DATE('17-12-1980','dd-mm-yyyy'),  800,  NULL, 20),
(7499, 'ALLEN',  'SALESMAN',  7698, TO_DATE('20-02-1981','dd-mm-yyyy'), 1600,   300, 30),
(7521, 'WARD',   'SALESMAN',  7698, TO_DATE('22-02-1981','dd-mm-yyyy'), 1250,   500, 30),
(7566, 'JONES',  'MANAGER',   7839, TO_DATE('02-04-1981','dd-mm-yyyy'), 2975,  NULL, 20),
(7654, 'MARTIN', 'SALESMAN',  7698, TO_DATE('28-09-1981','dd-mm-yyyy'), 1250,  1400, 30),
(7698, 'BLAKE',  'MANAGER',   7839, TO_DATE('01-05-1981','dd-mm-yyyy'), 2850,  NULL, 30),
(7782, 'CLARK',  'MANAGER',   7839, TO_DATE('09-06-1981','dd-mm-yyyy'), 2450,  NULL, 10),
(7788, 'SCOTT',  'ANALYST',   7566, TO_DATE('19-04-1987','dd-mm-yyyy'), 3000,  NULL, 20),
(7839, 'KING',   'PRESIDENT', NULL, TO_DATE('17-11-1981','dd-mm-yyyy'), 5000,  NULL, 10),
(7844, 'TURNER', 'SALESMAN',  7698, TO_DATE('08-09-1981','dd-mm-yyyy'), 1500,     0, 30),
(7876, 'ADAMS',  'CLERK',     7788, TO_DATE('23-05-1987','dd-mm-yyyy'), 1100,  NULL, 20),
(7900, 'JAMES',  'CLERK',     7698, TO_DATE('03-12-1981','dd-mm-yyyy'),  950,  NULL, 30),
(7902, 'FORD',   'ANALYST',   7566, TO_DATE('03-12-1981','dd-mm-yyyy'), 3000,  NULL, 20),
(7934, 'MILLER', 'CLERK',     7782, TO_DATE('23-01-1982','dd-mm-yyyy'), 1300,  NULL, 10);
