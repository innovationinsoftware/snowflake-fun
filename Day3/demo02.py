# python -m pip install -r requirements.txt

from pathlib import Path
import snowflake.connector
from vars import _pwd, _username, _account


stages = ['~', '%MOVIES', 'MOVIES_STAGE']
movies_path = Path(__file__).resolve().parent / "movies.csv"
# Snowflake PUT on Windows expects file://C:/..., not Path.as_uri()'s
# file:///C:/... form. Keep spaces literal; SQL quotes protect the path.
file_path = "file://" + movies_path.as_posix()
commands = ["USE ROLE SYSADMIN", "USE DATABASE MOVIES_DB", "USE SCHEMA MOVIES_SCHEMA"]

commands += [f"PUT '{file_path}' @{stage} auto_compress=false" for stage in stages]

download_dir = Path(__file__).resolve().parent / "downloads"
download_dir.mkdir(exist_ok=True)
download_path = "file://" + download_dir.as_posix() + "/"
commands += [f"GET @~/movies.csv '{download_path}'"]

with snowflake.connector.connect(user=_username, password=_pwd, account=_account, warehouse="compute_wh") as con:
    with con.cursor() as cur:
        try:
            for sql in commands:
                cur.execute(sql)
                print("Executed: ", sql)

        except Exception as ex:
            print(ex)
