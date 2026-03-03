# pip install snowflake-connector-python

import snowflake.connector
from vars import _pwd, _username

_account = 'pplliay-sdc01146'

with snowflake.connector.connect(user=_username, password=_pwd, account=_account, warehouse="compute_wh") as con:
    with con.cursor() as cur:
        try:
            cur.execute("use role accountadmin")
            cur.execute("use demo_db")
            cur.execute("SELECT * FROM scott.dept")
            rows = cur.fetchall()
            for row in rows:
                print(row)
        except Exception as ex:
            print(ex)
