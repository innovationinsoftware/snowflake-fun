# pip install snowflake-connector-python

import snowflake.connector
from vars import _pwd, _username

_account = 'pplliay-sdc01146'

stages = ['~', '%MOVIES', 'MOVIES_STAGE']
file_path = r"C:\Personal\Training\movies.csv"
commands = ["USE ROLE SYSADMIN", "USE DATABASE MOVIES_DB", "USE SCHEMA MOVIES_SCHEMA"]

commands += [f"PUT file://{file_path} @{stage} auto_compress=false" for stage in stages]

with snowflake.connector.connect(user=_username, password=_pwd, account=_account, warehouse="compute_wh") as con:
    with con.cursor() as cur:
        try:
            for sql in commands:
                cur.execute(sql)
                print("Executed: ", sql)

        except Exception as ex:
            print(ex)
