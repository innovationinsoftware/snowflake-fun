# python -m pip install -r requirements.txt

import snowflake.connector
from vars import _pwd, _username, _account


with snowflake.connector.connect(user=_username, password=_pwd, account=_account, warehouse="compute_wh") as con:
    with con.cursor() as cur:
        try:
            cur.execute("use role sysadmin")
            cur.execute("use demo_db")
            cur.execute("SELECT * FROM scott.dept")
            rows = cur.fetchall()
            for row in rows:
                print(row)
        except Exception as ex:
            print(ex)
